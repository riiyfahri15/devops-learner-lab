"""
TechnoDev - WebSocket API Lambda Function
LKS Nasional 2026 · Cloud Computing · Modul 3

Handles WebSocket $connect, $disconnect, and $default routes.
API Gateway WebSocket API: devops-ws-api

Routes:
  $connect     - Store connection in DB, track user session
  $disconnect  - Remove connection from DB
  $default     - Route messages (subscribe, unsubscribe, broadcast, notify)
  sendMessage  - Send message to specific connection

Features:
  - Real-time order status updates
  - Real-time notifications
  - Connection tracking in PostgreSQL
  - Broadcasting to all connected clients
  - Targeted messaging to specific users
"""

import base64
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

# Module-level clients for Lambda connection reuse
API_GW_CLIENT = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=os.environ.get(
        "WEBSOCKET_ENDPOINT",
        "https://placeholder.execute-api.us-west-2.amazonaws.com/prod",
    ),
)
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


def send_to_connection(connection_id, data):
    """Send a message to a specific WebSocket connection."""
    try:
        API_GW_CLIENT.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data, default=str).encode("utf-8"),
        )
        return True
    except API_GW_CLIENT.exceptions.GoneException:
        return False
    except Exception as e:
        print(f"Failed to send to connection {connection_id}: {e}")
        return False


def broadcast_to_all(conn, data, exclude_connection=None):
    """Broadcast a message to all active WebSocket connections."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT connection_id FROM ws_connections ORDER BY connected_at DESC"
        )
        connections = cur.fetchall()

    sent_count = 0
    failed_ids = []
    for c in connections:
        cid = c["connection_id"]
        if cid == exclude_connection:
            continue
        success = send_to_connection(cid, data)
        if success:
            sent_count += 1
        else:
            failed_ids.append(cid)

    # Clean up dead connections
    if failed_ids:
        cleanup_dead_connections(conn, failed_ids)

    return sent_count


def cleanup_dead_connections(conn, connection_ids):
    """Remove dead connections from database."""
    if not connection_ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ws_connections WHERE connection_id = ANY(%s)",
                (connection_ids,),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to cleanup connections: {e}")


def validate_ws_token(event):
    """Validate Cognito JWT token from WebSocket query parameter."""
    try:
        token = event.get("queryStringParameters", {}).get("token", "")
        if not token:
            return {}
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return {
            "sub": claims.get("sub", ""),
            "email": claims.get("email", ""),
            "cognito:username": claims.get("cognito:username", ""),
        }
    except Exception:
        return {}


def handle_connect(conn, connection_id, query_params, ws_claims=None):
    """Handle $connect route - store connection in database."""
    username = (
        (ws_claims or {}).get("cognito:username")
        or (ws_claims or {}).get("email")
        or query_params.get("username", "anonymous")
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO ws_connections (connection_id, username, route_key, connected_at) "
                "VALUES (%s, %s, '$connect', %s) "
                "RETURNING connection_id, username, connected_at",
                (connection_id, username, datetime.now(timezone.utc)),
            )
            ws_conn = cur.fetchone()
            conn.commit()

        print(f"WebSocket connected: {connection_id} (user={username})")

        # Notify all clients about new connection
        broadcast_to_all(
            conn,
            {
                "action": "user_connected",
                "connection_id": connection_id,
                "username": username,
                "timestamp": str(ws_conn["connected_at"]),
            },
            exclude_connection=connection_id,
        )

        return {"statusCode": 200, "body": "Connected"}
    except Exception as e:
        conn.rollback()
        print(f"Connect error: {e}")
        return {"statusCode": 500, "body": f"Connect failed: {str(e)}"}


def handle_disconnect(conn, connection_id):
    """Handle $disconnect route - remove connection from database."""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT username FROM ws_connections WHERE connection_id = %s",
                (connection_id,),
            )
            ws_conn = cur.fetchone()
            username = ws_conn["username"] if ws_conn else "unknown"

            cur.execute(
                "DELETE FROM ws_connections WHERE connection_id = %s",
                (connection_id,),
            )
            conn.commit()

        print(f"WebSocket disconnected: {connection_id} (user={username})")

        # Notify remaining clients
        broadcast_to_all(
            conn,
            {
                "action": "user_disconnected",
                "connection_id": connection_id,
                "username": username,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        return {"statusCode": 200, "body": "Disconnected"}
    except Exception as e:
        conn.rollback()
        print(f"Disconnect error: {e}")
        return {"statusCode": 500, "body": f"Disconnect failed: {str(e)}"}


def handle_default(conn, connection_id, body):
    """Handle $default route - route based on action in message body."""
    action = body.get("action", "unknown")

    if action == "ping":
        send_to_connection(
            connection_id,
            {"action": "pong", "timestamp": datetime.now(timezone.utc).isoformat()},
        )
        return {"statusCode": 200, "body": "pong"}

    elif action == "get_connections":
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT connection_id, username, connected_at FROM ws_connections "
                "ORDER BY connected_at DESC"
            )
            connections = cur.fetchall()

        send_to_connection(
            connection_id,
            {
                "action": "connections_list",
                "connections": connections,
                "total": len(connections),
            },
        )
        return {"statusCode": 200, "body": "connections_list sent"}

    elif action == "get_orders":
        user_id = body.get("user_id")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if user_id:
                cur.execute(
                    "SELECT o.id, o.user_id, o.quantity, o.total_price, o.status, "
                    "o.created_at, u.username, p.name AS product_name "
                    "FROM orders o "
                    "JOIN users u ON o.user_id = u.id "
                    "JOIN products p ON o.product_id = p.id "
                    "WHERE o.user_id = %s "
                    "ORDER BY o.created_at DESC LIMIT 20",
                    (user_id,),
                )
            else:
                cur.execute(
                    "SELECT o.id, o.user_id, o.quantity, o.total_price, o.status, "
                    "o.created_at, u.username, p.name AS product_name "
                    "FROM orders o "
                    "JOIN users u ON o.user_id = u.id "
                    "JOIN products p ON o.product_id = p.id "
                    "ORDER BY o.created_at DESC LIMIT 20"
                )
            orders = cur.fetchall()

        send_to_connection(
            connection_id,
            {"action": "orders_list", "orders": orders, "total": len(orders)},
        )
        return {"statusCode": 200, "body": "orders_list sent"}

    elif action == "order_status_update":
        order_id = body.get("order_id")
        new_status = body.get("status")
        if order_id and new_status:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM update_order_status(%s, %s)",
                        (order_id, new_status),
                    )
                    order = cur.fetchone()
                    conn.commit()

                if order:
                    # Broadcast to all connected clients
                    broadcast_to_all(
                        conn,
                        {
                            "action": "order_status_changed",
                            "order_id": order_id,
                            "new_status": new_status,
                            "user_id": str(order.get("user_id", "")),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    return {"statusCode": 200, "body": "Order status updated"}
            except Exception as e:
                conn.rollback()
                print(f"Order status update error: {e}")
                return {"statusCode": 500, "body": str(e)}

    elif action == "send_notification":
        message = body.get("message", "")
        target_user = body.get("target_user")
        notification_data = {
            "action": "notification",
            "message": message,
            "from": connection_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if target_user:
            # Send to specific user
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT connection_id FROM ws_connections WHERE username = %s",
                    (target_user,),
                )
                targets = cur.fetchall()
            for t in targets:
                send_to_connection(t["connection_id"], notification_data)
            return {"statusCode": 200, "body": f"Notification sent to {target_user}"}
        else:
            broadcast_to_all(conn, notification_data, exclude_connection=connection_id)
            return {"statusCode": 200, "body": "Notification broadcasted"}

    else:
        send_to_connection(
            connection_id,
            {
                "action": "error",
                "message": f"Unknown action: {action}",
                "received": body,
            },
        )
        return {"statusCode": 200, "body": f"Unknown action: {action}"}

    return {"statusCode": 200, "body": "OK"}


def handle_send_message(conn, connection_id, body):
    """Handle sendMessage route - targeted message to a specific connection."""
    target_connection = body.get("target_connection")
    message = body.get("message", "")
    sender = body.get("sender", connection_id)

    if not target_connection:
        return {"statusCode": 400, "body": "target_connection required"}

    success = send_to_connection(
        target_connection,
        {
            "action": "direct_message",
            "from": sender,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    if success:
        return {"statusCode": 200, "body": "Message sent"}
    else:
        # Target connection is gone, clean up
        cleanup_dead_connections(conn, [target_connection])
        return {"statusCode": 410, "body": "Target connection gone"}


def lambda_handler(event, context):
    """
    Main Lambda handler for WebSocket API Gateway.
    Routes based on $connect, $disconnect, $default, sendMessage.
    """
    route_key = event.get("requestContext", {}).get("routeKey", "$default")
    connection_id = event.get("requestContext", {}).get("connectionId", "unknown")
    query_params = event.get("queryStringParameters") or {}

    print(f"WebSocket event: route={route_key}, connection={connection_id}")

    # Reinitialize API GW client with actual domain
    domain = event.get("requestContext", {}).get("domainName", "")
    stage = event.get("requestContext", {}).get("stage", "prod")
    if domain:
        global API_GW_CLIENT
        API_GW_CLIENT = boto3.client(
            "apigatewaymanagementapi",
            endpoint_url=f"https://{domain}/{stage}",
        )

    conn = None
    try:
        conn = get_db_connection()

        if route_key == "$connect":
            ws_claims = validate_ws_token(event)
            return handle_connect(conn, connection_id, query_params, ws_claims=ws_claims)

        elif route_key == "$disconnect":
            return handle_disconnect(conn, connection_id)

        elif route_key == "sendMessage":
            body_str = event.get("body", "{}")
            body = json.loads(body_str) if isinstance(body_str, str) else body_str
            return handle_send_message(conn, connection_id, body)

        else:
            # $default route
            body_str = event.get("body", "{}")
            body = json.loads(body_str) if isinstance(body_str, str) else body_str
            return handle_default(conn, connection_id, body)

    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        return {"statusCode": 503, "body": f"Database error: {str(e)}"}
    except Exception as e:
        print(f"WebSocket handler error: {e}")
        return {"statusCode": 500, "body": f"Internal error: {str(e)}"}
    finally:
        if conn:
            conn.close()