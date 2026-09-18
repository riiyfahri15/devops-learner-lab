"""
TechnoDev - Order API Lambda Function
LKS Nasional 2026 · Cloud Computing · Modul 3

Handles CRUD operations for orders via API Gateway HTTP API.
Publishes order events to SQS for downstream processing.
Routes: ANY /orders, ANY /orders/{id}
"""

import json
import os
import uuid

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor


# Module-level SQS client for Lambda connection reuse
SQS_CLIENT = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-west-2"))


def get_cognito_claims(event):
    """Extract Cognito claims from API Gateway authorizer context."""
    try:
        claims = event["requestContext"]["authorizer"]["claims"]
        return {
            "sub": claims.get("sub", ""),
            "email": claims.get("email", ""),
            "cognito:username": claims.get("cognito:username", ""),
        }
    except (KeyError, TypeError):
        return {}


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


def get_queue_url(queue_name):
    """Resolve SQS queue URL by name."""
    try:
        resp = SQS_CLIENT.get_queue_url(QueueName=queue_name)
        return resp["QueueUrl"]
    except SQS_CLIENT.exceptions.QueueDoesNotExist:
        return None


def publish_to_sqs(queue_url, message_body, message_attributes=None):
    """Publish a message to SQS queue."""
    if queue_url is None:
        return
    params = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(message_body, default=str),
    }
    if message_attributes:
        params["MessageAttributes"] = message_attributes
    SQS_CLIENT.send_message(**params)


def response(status_code, body):
    """Build API Gateway HTTP API response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(body, default=str),
    }


def parse_body(event):
    """Parse request body from API Gateway event."""
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, str):
        return json.loads(body)
    return body


def list_orders(conn, query_params):
    """GET /orders - List all orders with optional filtering."""
    limit = int(query_params.get("limit", 50))
    offset = int(query_params.get("offset", 0))
    user_id = query_params.get("user_id")
    status_filter = query_params.get("status")

    base_query = (
        "SELECT o.id, o.user_id, o.product_id, o.quantity, o.total_price, "
        "o.status, o.shipping_address, o.notes, o.created_at, o.updated_at, "
        "u.username, u.email, p.name AS product_name, p.sku "
        "FROM orders o "
        "JOIN users u ON o.user_id = u.id "
        "JOIN products p ON o.product_id = p.id"
    )
    count_query = "SELECT COUNT(*) AS total FROM orders o"
    conditions = []
    params = []

    if user_id:
        conditions.append("o.user_id = %s")
        params.append(user_id)
    if status_filter:
        conditions.append("o.status = %s")
        params.append(status_filter)

    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        base_query += where_clause
        count_query += where_clause

    base_query += " ORDER BY o.created_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    count_params = list(params[:-2])  # exclude limit/offset

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(base_query, params)
        orders = cur.fetchall()

        cur.execute(count_query, count_params)
        total = cur.fetchone()["total"]

    return response(200, {"orders": orders, "total": total, "limit": limit, "offset": offset})


def list_products(conn, query_params):
    """GET /products - List product catalog with optional pagination."""
    limit = int(query_params.get("limit", 50))
    offset = int(query_params.get("offset", 0))

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, sku, name, description, price, stock, category, "
            "created_at, updated_at FROM products "
            "ORDER BY created_at ASC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        products = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS total FROM products")
        total = cur.fetchone()["total"]

    return response(200, {"products": products, "total": total, "limit": limit, "offset": offset})


def get_product(conn, product_id):
    """GET /products/{id} - Get a single product by ID."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, sku, name, description, price, stock, category, "
            "created_at, updated_at FROM products WHERE id = %s",
            (product_id,),
        )
        product = cur.fetchone()

    if product is None:
        return response(404, {"error": "Product not found", "product_id": product_id})

    return response(200, {"product": product})


def get_order(conn, order_id):
    """GET /orders/{id} - Get a single order by ID."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT o.id, o.user_id, o.product_id, o.quantity, o.total_price, "
            "o.status, o.shipping_address, o.notes, o.created_at, o.updated_at, "
            "u.username, u.email, p.name AS product_name, p.sku, p.price AS unit_price "
            "FROM orders o "
            "JOIN users u ON o.user_id = u.id "
            "JOIN products p ON o.product_id = p.id "
            "WHERE o.id = %s",
            (order_id,),
        )
        order = cur.fetchone()

    if order is None:
        return response(404, {"error": "Order not found", "order_id": order_id})

    return response(200, {"order": order})


def create_order(conn, body):
    """POST /orders - Create a new order and publish to SQS."""
    required_fields = ["user_id", "product_id", "quantity"]
    for field in required_fields:
        if field not in body:
            return response(400, {"error": f"Missing required field: {field}"})

    order_id = str(uuid.uuid4())
    user_id = body["user_id"]
    product_id = body["product_id"]
    quantity = int(body["quantity"])

    if quantity <= 0:
        return response(400, {"error": "Quantity must be greater than 0"})

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            # Verify user exists
            cur.execute("SELECT id, username, email FROM users WHERE id = %s", (user_id,))
            user = cur.fetchone()
            if user is None:
                conn.rollback()
                return response(404, {"error": "User not found", "user_id": user_id})

            # Get product details and check stock (with row lock)
            cur.execute(
                "SELECT id, name, sku, price, stock FROM products WHERE id = %s FOR UPDATE",
                (product_id,),
            )
            product = cur.fetchone()
            if product is None:
                conn.rollback()
                return response(404, {"error": "Product not found", "product_id": product_id})

            if product["stock"] < quantity:
                conn.rollback()
                return response(
                    400,
                    {
                        "error": "Insufficient stock",
                        "available": product["stock"],
                        "requested": quantity,
                    },
                )

            total_price = float(product["price"]) * quantity

            # Create order
            cur.execute(
                "INSERT INTO orders (id, user_id, product_id, quantity, total_price, "
                "status, shipping_address, notes) "
                "VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s) "
                "RETURNING id, user_id, product_id, quantity, total_price, status, "
                "shipping_address, notes, created_at, updated_at",
                (
                    order_id,
                    user_id,
                    product_id,
                    quantity,
                    total_price,
                    body.get("shipping_address"),
                    body.get("notes"),
                ),
            )
            order = cur.fetchone()

            # Decrease product stock
            cur.execute(
                "UPDATE products SET stock = stock - %s, updated_at = NOW() WHERE id = %s",
                (quantity, product_id),
            )

            # Create notification record in DB
            notif_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO notifications (id, user_id, order_id, type, message, channel, status) "
                "VALUES (%s, %s, %s, 'order_created', %s, 'email', 'pending')",
                (
                    notif_id,
                    user_id,
                    order_id,
                    f"Your order #{order_id[:8]} for {product['name']} x{quantity} has been placed successfully.",
                ),
            )

            conn.commit()

        except psycopg2.errors.ForeignKeyViolation as e:
            conn.rollback()
            return response(400, {"error": "Invalid reference", "detail": str(e)})
        except Exception as e:
            conn.rollback()
            return response(500, {"error": "Failed to create order", "detail": str(e)})

    # Publish to SQS after successful commit (best-effort)
    try:
        orders_queue_url = get_queue_url(os.environ.get("ORDERS_QUEUE", "devops-orders-queue"))
        if orders_queue_url:
            publish_to_sqs(
                orders_queue_url,
                {
                    "event": "order_created",
                    "order_id": order_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "total_price": total_price,
                    "user_name": user["username"],
                    "user_email": user["email"],
                    "product_name": product["name"],
                    "product_sku": product["sku"],
                },
                message_attributes={
                    "OrderType": {"DataType": "String", "StringValue": "new_order"},
                    "OrderId": {"DataType": "String", "StringValue": order_id},
                },
            )

        # Also publish to notifications queue for notification-worker
        notif_queue_url = get_queue_url(os.environ.get("NOTIFICATIONS_QUEUE", "devops-notifications-queue"))
        if notif_queue_url:
            publish_to_sqs(
                notif_queue_url,
                {
                    "event": "notification",
                    "notification_id": notif_id,
                    "user_id": user_id,
                    "order_id": order_id,
                    "type": "order_created",
                    "message": f"Order #{order_id[:8]} created successfully",
                    "user_email": user["email"],
                    "user_name": user["username"],
                },
            )
    except Exception as sqs_err:
        # SQS publish is best-effort; order already committed to DB
        print(f"SQS publish warning: {sqs_err}")

    return response(201, {"message": "Order created successfully", "order": order})


def update_order_status(conn, order_id, body):
    """PUT /orders/{id} - Update order status using stored procedure."""
    new_status = body.get("status")
    if not new_status:
        return response(400, {"error": "Missing required field: status"})

    valid_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    if new_status not in valid_statuses:
        return response(
            400,
            {"error": f"Invalid status. Must be one of: {', '.join(valid_statuses)}"},
        )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT * FROM update_order_status(%s, %s)", (order_id, new_status))
            order = cur.fetchone()
            conn.commit()
        except Exception as e:
            conn.rollback()
            return response(500, {"error": "Failed to update order status", "detail": str(e)})

    if order is None:
        return response(404, {"error": "Order not found", "order_id": order_id})

    # Publish status update notification
    try:
        notif_queue_url = get_queue_url(os.environ.get("NOTIFICATIONS_QUEUE", "devops-notifications-queue"))
        if notif_queue_url:
            publish_to_sqs(
                notif_queue_url,
                {
                    "event": "order_status_updated",
                    "order_id": order_id,
                    "user_id": str(order["user_id"]),
                    "status": new_status,
                    "message": f"Order #{order_id[:8]} status updated to {new_status}",
                },
            )
    except Exception as sqs_err:
        print(f"SQS publish warning: {sqs_err}")

    return response(200, {"message": "Order status updated", "order": order})


def delete_order(conn, order_id):
    """DELETE /orders/{id} - Cancel/delete an order (restores stock)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                "SELECT id, product_id, quantity, status FROM orders WHERE id = %s",
                (order_id,),
            )
            order = cur.fetchone()

            if order is None:
                return response(404, {"error": "Order not found", "order_id": order_id})

            if order["status"] not in ("pending", "cancelled"):
                return response(
                    400,
                    {
                        "error": f"Cannot delete order with status '{order['status']}'. "
                        "Only pending or cancelled orders can be deleted."
                    },
                )

            # Restore stock
            cur.execute(
                "UPDATE products SET stock = stock + %s, updated_at = NOW() WHERE id = %s",
                (order["quantity"], order["product_id"]),
            )

            cur.execute("DELETE FROM orders WHERE id = %s RETURNING id", (order_id,))
            deleted = cur.fetchone()
            conn.commit()

        except Exception as e:
            conn.rollback()
            return response(500, {"error": "Failed to delete order", "detail": str(e)})

    return response(200, {"message": "Order deleted successfully", "order_id": str(deleted["id"])})


def lambda_handler(event, context):
    """Main Lambda handler for Order API. Also serves the read-only Products catalog."""
    http_method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "GET")
    )
    path = (
        event.get("path")
        or event.get("requestContext", {}).get("http", {}).get("path", "/orders")
    )
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    # Extract authenticated user from Cognito
    claims = get_cognito_claims(event)
    if claims.get('sub'):
        print(f"Authenticated user: {claims.get('cognito:username', claims.get('sub'))}")

    # Handle CORS preflight
    if http_method == "OPTIONS":
        return response(200, {"message": "OK"})

    is_products_route = path.startswith("/products")

    conn = None
    try:
        conn = get_db_connection()

        if is_products_route:
            product_id = path_params.get("id")
            if http_method == "GET" and product_id is None:
                return list_products(conn, query_params)
            elif http_method == "GET" and product_id:
                return get_product(conn, product_id)
            else:
                return response(405, {"error": f"Method {http_method} not allowed on {path}"})

        order_id = path_params.get("id")

        if http_method == "GET" and order_id is None:
            return list_orders(conn, query_params)
        elif http_method == "GET" and order_id:
            return get_order(conn, order_id)
        elif http_method == "POST":
            body = parse_body(event)
            return create_order(conn, body)
        elif http_method == "PUT" and order_id:
            body = parse_body(event)
            return update_order_status(conn, order_id, body)
        elif http_method == "DELETE" and order_id:
            return delete_order(conn, order_id)
        else:
            return response(405, {"error": f"Method {http_method} not allowed"})

    except psycopg2.OperationalError as e:
        return response(503, {"error": "Database connection failed", "detail": str(e)})
    except Exception as e:
        return response(500, {"error": "Internal server error", "detail": str(e)})
    finally:
        if conn:
            conn.close()