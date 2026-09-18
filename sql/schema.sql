-- =============================================================================
-- TechnoDev Database Schema - PostgreSQL
-- LKS Nasional 2026 · Cloud Computing · Modul 3
-- Database: devopsdb
-- =============================================================================

-- Drop tables if exist (idempotent)
DROP TABLE IF EXISTS ws_connections CASCADE;
DROP TABLE IF EXISTS notifications CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Drop stored procedures if exist
DROP FUNCTION IF EXISTS update_order_status(UUID, VARCHAR);
DROP FUNCTION IF EXISTS get_user_orders_summary(UUID);

-- =============================================================================
-- TABLES
-- =============================================================================

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    category VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    total_price NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    shipping_address TEXT,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    order_id UUID REFERENCES orders(id) ON DELETE SET NULL,
    type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    channel VARCHAR(20) NOT NULL DEFAULT 'email' CHECK (channel IN ('email', 'sms', 'push')),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed', 'read')),
    sent_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE ws_connections (
    connection_id VARCHAR(100) PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    username VARCHAR(50),
    route_key VARCHAR(20) DEFAULT '$default',
    connected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Users indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);

-- Products indexes
CREATE INDEX idx_products_sku ON products(sku);

-- Orders indexes
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_orders_status ON orders(status);

-- Notifications indexes
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_order_id ON notifications(order_id);
CREATE INDEX idx_notifications_status ON notifications(status);

CREATE INDEX idx_ws_connections_user_id ON ws_connections(user_id);
CREATE INDEX idx_ws_connections_connected_at ON ws_connections(connected_at);

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================

-- Procedure: update_order_status
-- Updates the status of an order and sets the updated_at timestamp
CREATE OR REPLACE FUNCTION update_order_status(
    p_order_id UUID,
    p_new_status VARCHAR
) RETURNS orders AS $$
DECLARE
    result orders%ROWTYPE;
BEGIN
    UPDATE orders
    SET status = p_new_status,
        updated_at = NOW()
    WHERE id = p_order_id
    RETURNING * INTO result;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Order % not found', p_order_id;
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Procedure: get_user_orders_summary
-- Returns a summary of all orders for a given user
CREATE OR REPLACE FUNCTION get_user_orders_summary(
    p_user_id UUID
) RETURNS TABLE (
    user_id UUID,
    username VARCHAR,
    email VARCHAR,
    total_orders BIGINT,
    total_spent NUMERIC,
    pending_orders BIGINT,
    processing_orders BIGINT,
    shipped_orders BIGINT,
    delivered_orders BIGINT,
    cancelled_orders BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        u.id AS user_id,
        u.username,
        u.email,
        COUNT(o.id) AS total_orders,
        COALESCE(SUM(o.total_price), 0) AS total_spent,
        COUNT(o.id) FILTER (WHERE o.status = 'pending') AS pending_orders,
        COUNT(o.id) FILTER (WHERE o.status = 'processing') AS processing_orders,
        COUNT(o.id) FILTER (WHERE o.status = 'shipped') AS shipped_orders,
        COUNT(o.id) FILTER (WHERE o.status = 'delivered') AS delivered_orders,
        COUNT(o.id) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders
    FROM users u
    LEFT JOIN orders o ON o.user_id = u.id
    WHERE u.id = p_user_id
    GROUP BY u.id, u.username, u.email;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- Sample Users (3)
INSERT INTO users (id, username, email, full_name, phone, address) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'john_doe', 'john.doe@technodev.com', 'John Doe', '+6281234567890', 'Jl. Sudirman No. 123, Jakarta'),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'jane_smith', 'jane.smith@technodev.com', 'Jane Smith', '+6282345678901', 'Jl. Thamrin No. 456, Jakarta'),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', 'bob_wilson', 'bob.wilson@technodev.com', 'Bob Wilson', '+6283456789012', 'Jl. Gatot Subroto No. 789, Jakarta');

-- Sample Products (5)
INSERT INTO products (id, sku, name, description, price, stock, category) VALUES
    ('d4e5f6a7-b8c9-0123-defa-234567890123', 'TECH-LAPTOP-001', 'TechnoDev Laptop Pro 15', 'High-performance laptop with 16GB RAM, 512GB SSD, Intel Core i7', 15999000.00, 50, 'Electronics'),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', 'TECH-PHONE-002', 'TechnoDev Smartphone X', 'Flagship smartphone with 6.7" AMOLED, 128GB, 5G capable', 8999000.00, 100, 'Electronics'),
    ('f6a7b8c9-d0e1-2345-fabc-456789012345', 'TECH-TABLET-003', 'TechnoDev Tablet Air 11', 'Lightweight tablet with stylus support, 256GB storage', 6499000.00, 75, 'Electronics'),
    ('a7b8c9d0-e1f2-3456-abcd-567890123456', 'TECH-AUDIO-004', 'TechnoDev Wireless Earbuds Pro', 'Active noise cancellation, 24hr battery, IPX5 waterproof', 1299000.00, 200, 'Audio'),
    ('b8c9d0e1-f2a3-4567-bcde-678901234567', 'TECH-WATCH-005', 'TechnoDev Smartwatch Ultra', 'Health monitoring, GPS, 7-day battery, AMOLED display', 3499000.00, 150, 'Wearables');