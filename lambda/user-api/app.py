"""
TechnoDev - User API Lambda Function
LKS Nasional 2026 · Cloud Computing · Modul 3

Handles CRUD operations for users via API Gateway REST API.
Routes: ANY /users, ANY /users/{id}

Also supports a direct-invoke maintenance action, {"action": "bootstrap_schema"},
used by deploy.sh to apply sql/schema.sql (bundled into the image as schema.sql)
against RDS without needing SSH/SSM access to a runner instance.
"""

import json
import logging
import os
import uuid

import psycopg2
from psycopg2.extras import RealDictCursor

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_cognito_claims(event):
    """Extract Cognito JWT claims from API Gateway event."""
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
    """Create a database connection using environment variables or Secrets Manager."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    dbname = os.environ.get("DB_NAME", "devopsdb")
    user = os.environ.get("DB_USER", os.environ.get("DB_USERNAME", "devopsadmin"))
    password = os.environ.get("DB_PASSWORD", "password")
    sslmode = os.environ.get("DB_SSLMODE", "prefer")

    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


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
    if body is None or body == "":
        return {}
    if isinstance(body, str):
        return json.loads(body)
    return body


def list_users(conn, query_params):
    """GET /users - List all users with optional pagination."""
    limit = int(query_params.get("limit", 50))
    offset = int(query_params.get("offset", 0))

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, username, email, full_name, phone, address, "
            "created_at, updated_at FROM users "
            "ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        users = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS total FROM users")
        total = cur.fetchone()["total"]

    return response(200, {"users": users, "total": total, "limit": limit, "offset": offset})


def get_user(conn, user_id):
    """GET /users/{id} - Get a single user by ID."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id, username, email, full_name, phone, address, "
            "created_at, updated_at FROM users WHERE id = %s",
            (user_id,),
        )
        user = cur.fetchone()

    if user is None:
        return response(404, {"error": "User not found", "user_id": user_id})

    return response(200, {"user": user})


def create_user(conn, body):
    """POST /users - Create a new user."""
    required_fields = ["username", "email", "full_name"]
    for field in required_fields:
        if field not in body:
            return response(400, {"error": f"Missing required field: {field}"})

    user_id = str(uuid.uuid4())

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                "INSERT INTO users (id, username, email, full_name, phone, address) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "RETURNING id, username, email, full_name, phone, address, created_at, updated_at",
                (
                    user_id,
                    body["username"],
                    body["email"],
                    body["full_name"],
                    body.get("phone"),
                    body.get("address"),
                ),
            )
            user = cur.fetchone()
            conn.commit()
        except psycopg2.errors.UniqueViolation as e:
            conn.rollback()
            return response(409, {"error": "User already exists", "detail": str(e)})
        except Exception as e:
            conn.rollback()
            return response(500, {"error": "Failed to create user", "detail": str(e)})

    return response(201, {"message": "User created successfully", "user": user})


def update_user(conn, user_id, body):
    """PUT /users/{id} - Update an existing user."""
    if not body:
        return response(400, {"error": "Request body is required"})

    allowed_fields = ["username", "email", "full_name", "phone", "address"]
    updates = {k: v for k, v in body.items() if k in allowed_fields and v is not None}

    if not updates:
        return response(400, {"error": "No valid fields to update"})

    set_clauses = [f"{k} = %s" for k in updates.keys()]
    set_clauses.append("updated_at = NOW()")
    values = list(updates.values())
    values.append(user_id)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                f"UPDATE users SET {', '.join(set_clauses)} "
                "WHERE id = %s "
                "RETURNING id, username, email, full_name, phone, address, created_at, updated_at",
                values,
            )
            user = cur.fetchone()

            if user is None:
                conn.rollback()
                return response(404, {"error": "User not found", "user_id": user_id})

            conn.commit()
        except psycopg2.errors.UniqueViolation as e:
            conn.rollback()
            return response(409, {"error": "Conflict - duplicate value", "detail": str(e)})
        except Exception as e:
            conn.rollback()
            return response(500, {"error": "Failed to update user", "detail": str(e)})

    return response(200, {"message": "User updated successfully", "user": user})


def delete_user(conn, user_id):
    """DELETE /users/{id} - Delete a user."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("DELETE FROM users WHERE id = %s RETURNING id, username, email", (user_id,))
        user = cur.fetchone()

        if user is None:
            conn.rollback()
            return response(404, {"error": "User not found", "user_id": user_id})

        conn.commit()

    return response(200, {"message": "User deleted successfully", "user": user})


def bootstrap_schema(conn):
    """Apply sql/schema.sql (bundled as schema.sql in the image) against the connected
    database. Used for direct Lambda invoke with {"action": "bootstrap_schema"} since
    AWS Academy Learner Lab runner instances cannot always reach RDS reliably over SSM.
    """
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        tables = [row["table_name"] for row in cur.fetchall()]

    return response(200, {"message": "Schema bootstrap succeeded", "tables": tables})


def lambda_handler(event, context):
    """Main Lambda handler for User API."""
    # Direct-invoke maintenance action (not routed through API Gateway)
    if isinstance(event, dict) and event.get("action") == "bootstrap_schema":
        conn = None
        try:
            conn = get_db_connection()
            return bootstrap_schema(conn)
        except Exception as e:
            if conn:
                conn.rollback()
            return response(500, {"error": "Schema bootstrap failed", "detail": str(e)})
        finally:
            if conn:
                conn.close()

    # Extract authenticated user from Cognito
    claims = get_cognito_claims(event)
    if claims.get("sub"):
        logger.info("Authenticated user: %s", claims.get("cognito:username", claims.get("sub")))

    http_method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "GET")
    )
    path = event.get("path", "/users")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    # Handle CORS preflight
    if http_method == "OPTIONS":
        return response(200, {"message": "OK"})

    conn = None
    try:
        conn = get_db_connection()

        user_id = path_params.get("id")

        if http_method == "GET" and user_id is None:
            return list_users(conn, query_params)
        elif http_method == "GET" and user_id:
            return get_user(conn, user_id)
        elif http_method == "POST":
            body = parse_body(event)
            return create_user(conn, body)
        elif http_method == "PUT" and user_id:
            body = parse_body(event)
            return update_user(conn, user_id, body)
        elif http_method == "DELETE" and user_id:
            return delete_user(conn, user_id)
        else:
            return response(405, {"error": f"Method {http_method} not allowed on {path}"})

    except psycopg2.OperationalError as e:
        return response(503, {"error": "Database connection failed", "detail": str(e)})
    except Exception as e:
        return response(500, {"error": "Internal server error", "detail": str(e)})
    finally:
        if conn:
            conn.close()