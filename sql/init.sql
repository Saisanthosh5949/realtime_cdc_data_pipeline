CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    state VARCHAR(2),
    customer_segment VARCHAR(50),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers(first_name,last_name,email,state,customer_segment)
VALUES
('Ava','Miller','ava.miller@example.com','TX','Consumer'),
('Noah','Wilson','noah.wilson@example.com','CO','Corporate'),
('Mia','Davis','mia.davis@example.com','CA','Small Business')
ON CONFLICT DO NOTHING;
