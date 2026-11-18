\set ON_ERROR_STOP on
BEGIN;
REVOKE ALL ON DATABASE stokora FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA operational;

CREATE TABLE operational.tenants (
    tenant_id text PRIMARY KEY
);
CREATE TABLE operational.stores (
    tenant_id text NOT NULL REFERENCES operational.tenants,
    site_id text NOT NULL,
    PRIMARY KEY (tenant_id, site_id)
);
CREATE TABLE operational.products (
    tenant_id text NOT NULL REFERENCES operational.tenants,
    product_id text NOT NULL,
    PRIMARY KEY (tenant_id, product_id)
);
CREATE TABLE operational.stock_movements (
    tenant_id text NOT NULL,
    site_id text NOT NULL,
    movement_id text NOT NULL,
    product_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    quantity_signed integer NOT NULL CHECK (quantity_signed BETWEEN -1000000 AND 1000000 AND quantity_signed <> 0),
    unit text NOT NULL CHECK (unit = 'each'),
    is_simulated boolean NOT NULL,
    synthetic_scenario_id text,
    PRIMARY KEY (tenant_id, site_id, movement_id),
    FOREIGN KEY (tenant_id, site_id) REFERENCES operational.stores,
    FOREIGN KEY (tenant_id, product_id) REFERENCES operational.products,
    CHECK (is_simulated = (synthetic_scenario_id IS NOT NULL))
);
CREATE TABLE operational.sales_events (
    tenant_id text NOT NULL,
    site_id text NOT NULL,
    event_id text NOT NULL,
    product_id text NOT NULL,
    business_date date NOT NULL,
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000000),
    currency text NOT NULL CHECK (currency IN ('USD', 'MAD', 'GBP', 'EUR')),
    unit_price_minor integer NOT NULL CHECK (unit_price_minor BETWEEN 0 AND 1000000000),
    source text NOT NULL,
    is_simulated boolean NOT NULL,
    synthetic_scenario_id text,
    PRIMARY KEY (tenant_id, site_id, event_id),
    FOREIGN KEY (tenant_id, site_id) REFERENCES operational.stores,
    FOREIGN KEY (tenant_id, product_id) REFERENCES operational.products,
    CHECK (is_simulated = (synthetic_scenario_id IS NOT NULL))
);
CREATE TABLE operational.purchase_orders (
    tenant_id text NOT NULL,
    site_id text NOT NULL,
    purchase_order_id text NOT NULL,
    product_id text NOT NULL,
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 1000000),
    currency text NOT NULL CHECK (currency IN ('USD', 'MAD', 'GBP', 'EUR')),
    status text NOT NULL CHECK (status IN ('pending', 'approved', 'cancelled')),
    approved_by text,
    PRIMARY KEY (tenant_id, site_id, purchase_order_id),
    FOREIGN KEY (tenant_id, site_id) REFERENCES operational.stores,
    FOREIGN KEY (tenant_id, product_id) REFERENCES operational.products,
    CHECK (status <> 'approved' OR approved_by IS NOT NULL)
);
CREATE TABLE operational.audit (
    tenant_id text NOT NULL,
    site_id text NOT NULL,
    audit_id bigint GENERATED ALWAYS AS IDENTITY,
    action text NOT NULL,
    actor text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, site_id, audit_id),
    FOREIGN KEY (tenant_id, site_id) REFERENCES operational.stores
);

CREATE INDEX sales_product_date ON operational.sales_events (tenant_id, product_id, business_date);
CREATE INDEX stock_product_time ON operational.stock_movements (tenant_id, product_id, occurred_at);
CREATE INDEX purchase_product ON operational.purchase_orders (tenant_id, product_id);
ALTER TABLE operational.sales_events REPLICA IDENTITY FULL;
COMMIT;
