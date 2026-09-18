"""
TechnoDev - Notification Worker Lambda Function
LKS Nasional 2026 · Cloud Computing · Modul 3

Triggered by SQS (devops-notifications-queue).
Processes notification messages, updates DB records,
and publishes to SNS (devops-notifications) topic.
"""

import json
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

# Module-level clients for Lambda connection reuse
SNS_CLIENT = boto3.client("sns", region_name=os.environ.get("AWS_REGION", "us-west-2"))
SQS_CLIENT = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-west-2"))


def get_db_connection():
    """Create a database connection using environment variables."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    dbname = os.environ.get("DB_NAME", "devopsdb")
    user = os.environ.get("DB_USER", "devopsadmin")
    password = os.environ.get("DB_PASSWORD", "password")

    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


def get_sns_topic_arn(topic_name):
    """Resolve SNS topic ARN by name."""
    try:
        resp = SNS_CLIENT.create_topic(Name=topic_name)
        return resp["TopicArn"]
    except Exception as e:
        print(f"Failed to resolve SNS topic {topic_name}: {e}")
        return None


def publish_to_sns(topic_arn, subject, message, attributes=None):
    """Publish a message to SNS topic."""
    if topic_arn is None:
        return None

    params = {
        "TopicArn": topic_arn,
        "Subject": subject[:100],  # SNS subject max 100 chars
        "Message": json.dumps(message, default=str) if isinstance(message, dict) else message,
        "MessageAttributes": {
            "notification_type": {
                "DataType": "String",
                "StringValue": attributes.get("type", "general") if attributes else "general",
            },
        },
    }

    if attributes:
        for key, value in attributes.items():
            params["MessageAttributes"][key] = {
                "DataType": "String",
                "StringValue": str(value),
            }

    resp = SNS_CLIENT.publish(**params)
    return resp.get("MessageId")


def update_notification_status(conn, notification_id, status, sent_at=None):
    """Update notification record status in database."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status == "sent" and sent_at:
                cur.execute(
                    "UPDATE notifications SET status = %s, sent_at = %s WHERE id = %s "
                    "RETURNING id, user_id, order_id, type, message, status, sent_at",
                    (status, sent_at, notification_id),
                )
            else:
                cur.execute(
                    "UPDATE notifications SET status = %s WHERE id = %s "
                    "RETURNING id, user_id, order_id, type, message, status",
                    (status, notification_id),
                )
            result = cur.fetchone()
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        print(f"Failed to update notification status: {e}")
        return None


def create_notification_record(conn, message_data):
    """Create a notification record if one doesn't exist."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            user_id = message_data.get("user_id")
            order_id = message_data.get("order_id")
            notif_type = message_data.get("type", message_data.get("event", "general"))
            notif_message = message_data.get("message", "You have a new notification")

            cur.execute(
                "INSERT INTO notifications (id, user_id, order_id, type, message, channel, status) "
                "VALUES (gen_random_uuid(), %s, %s, %s, %s, 'email', 'pending') "
                "RETURNING id, user_id, order_id, type, message, status, created_at",
                (user_id, order_id, notif_type, notif_message),
            )
            notification = cur.fetchone()
            conn.commit()
            return notification
    except Exception as e:
        conn.rollback()
        print(f"Failed to create notification record: {e}")
        return None


def process_order_created(conn, message_data, notification_topic_arn):
    """Process order_created event notification."""
    notification_id = message_data.get("notification_id")
    user_email = message_data.get("user_email", "unknown")
    user_name = message_data.get("user_name", "Customer")
    order_id = message_data.get("order_id", "unknown")
    message_text = message_data.get("message", "")

    # If no notification_id, create one (fallback)
    if not notification_id:
        notification = create_notification_record(conn, message_data)
        if notification:
            notification_id = str(notification["id"])
            message_text = notification.get("message", message_text)

    subject = f"Order Confirmation - #{str(order_id)[:8]}"
    sns_message = {
        "default": message_text or f"Your order #{str(order_id)[:8]} has been placed successfully.",
        "email": (
            f"Hello {user_name},\n\n"
            f"Your order #{str(order_id)[:8]} has been placed successfully.\n\n"
            f"Order Details:\n"
            f"- Order ID: {order_id}\n"
            f"- Status: Pending\n\n"
            f"You will receive updates as your order progresses.\n\n"
            f"Best regards,\nTechnoDev Team"
        ),
        "sms": f"TechnoDev: Order #{str(order_id)[:8]} confirmed. We'll keep you updated!",
    }

    msg_id = publish_to_sns(
        notification_topic_arn,
        subject,
        sns_message,
        attributes={
            "type": "order_created",
            "order_id": str(order_id),
            "user_id": str(message_data.get("user_id", "")),
            "user_email": user_email,
        },
    )

    # Update notification status
    if notification_id:
        update_notification_status(conn, notification_id, "sent", sent_at=datetime.now(timezone.utc))

    print(f"Order created notification sent: user={user_email}, order={order_id}, sns_msg_id={msg_id}")
    return msg_id


def process_order_status_updated(conn, message_data, notification_topic_arn):
    """Process order_status_updated event notification."""
    notification_id = message_data.get("notification_id")
    order_id = message_data.get("order_id", "unknown")
    status = message_data.get("status", "unknown")
    user_id = message_data.get("user_id")
    message_text = message_data.get("message", "")

    # If no notification_id, create one
    if not notification_id:
        notification = create_notification_record(conn, message_data)
        if notification:
            notification_id = str(notification["id"])

    status_display = status.replace("_", " ").title()
    subject = f"Order Update - #{str(order_id)[:8]} is now {status_display}"
    sns_message = {
        "default": message_text or f"Order #{str(order_id)[:8]} status updated to {status_display}.",
        "email": (
            f"Hello,\n\n"
            f"Your order #{str(order_id)[:8]} has been updated.\n\n"
            f"New Status: {status_display}\n\n"
            f"Best regards,\nTechnoDev Team"
        ),
        "sms": f"TechnoDev: Order #{str(order_id)[:8]} is now {status_display}.",
    }

    msg_id = publish_to_sns(
        notification_topic_arn,
        subject,
        sns_message,
        attributes={
            "type": "order_status_updated",
            "order_id": str(order_id),
            "status": status,
            "user_id": str(user_id or ""),
        },
    )

    if notification_id:
        update_notification_status(conn, notification_id, "sent", sent_at=datetime.now(timezone.utc))

    print(f"Status update notification sent: order={order_id}, status={status}, sns_msg_id={msg_id}")
    return msg_id


def process_notification_event(conn, message_data, notification_topic_arn):
    """Process a generic notification event."""
    notification_id = message_data.get("notification_id")
    message_text = message_data.get("message", "You have a new notification")
    notif_type = message_data.get("type", message_data.get("event", "general"))

    # If no notification_id, create one
    if not notification_id:
        notification = create_notification_record(conn, message_data)
        if notification:
            notification_id = str(notification["id"])
            message_text = notification.get("message", message_text)

    subject = f"TechnoDev Notification - {notif_type.replace('_', ' ').title()}"
    sns_message = {
        "default": message_text,
        "email": (
            f"Hello,\n\n{message_text}\n\n"
            f"Best regards,\nTechnoDev Team"
        ),
        "sms": f"TechnoDev: {message_text}",
    }

    msg_id = publish_to_sns(
        notification_topic_arn,
        subject,
        sns_message,
        attributes={
            "type": notif_type,
            "notification_id": str(notification_id or ""),
        },
    )

    if notification_id:
        update_notification_status(conn, notification_id, "sent", sent_at=datetime.now(timezone.utc))

    print(f"Notification sent: type={notif_type}, sns_msg_id={msg_id}")
    return msg_id


def lambda_handler(event, context):
    """
    Main Lambda handler. Triggered by SQS (devops-notifications-queue).
    Processes each record and publishes to SNS.
    """
    notification_topic_arn = get_sns_topic_arn(
        os.environ.get("NOTIFICATION_TOPIC", "devops-notifications")
    )

    conn = None
    try:
        conn = get_db_connection()

        for record in event.get("Records", []):
            try:
                # Parse SQS message body
                body = record.get("body", "{}")
                if isinstance(body, str):
                    message_data = json.loads(body)
                else:
                    message_data = body

                event_type = message_data.get("event", message_data.get("type", "unknown"))

                print(f"Processing notification: event={event_type}, message_id={record.get('messageId')}")

                # Route to appropriate handler
                if event_type == "order_created":
                    process_order_created(conn, message_data, notification_topic_arn)
                elif event_type == "order_status_updated":
                    process_order_status_updated(conn, message_data, notification_topic_arn)
                elif event_type == "notification":
                    process_notification_event(conn, message_data, notification_topic_arn)
                else:
                    # Fallback: process as generic notification
                    print(f"Unknown event type '{event_type}', processing as generic notification")
                    process_notification_event(conn, message_data, notification_topic_arn)

            except json.JSONDecodeError as e:
                print(f"Failed to parse message body: {e}")
                continue
            except Exception as e:
                print(f"Failed to process record: {e}")
                continue

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Processed {len(event.get('Records', []))} notification(s) successfully"
            }),
        }

    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Database connection failed", "detail": str(e)}),
        }
    except Exception as e:
        print(f"Lambda execution error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "detail": str(e)}),
        }
    finally:
        if conn:
            conn.close()