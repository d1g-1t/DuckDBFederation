CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    subscription_tier VARCHAR(50) NOT NULL DEFAULT 'free',
    country_code VARCHAR(5) NOT NULL DEFAULT 'US',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (email, subscription_tier, country_code, is_active) VALUES
    ('alice@example.com',   'enterprise', 'US', true),
    ('bob@example.com',     'pro',        'GB', true),
    ('charlie@example.com', 'free',       'DE', true),
    ('diana@example.com',   'pro',        'JP', true),
    ('eve@example.com',     'enterprise', 'BR', false),
    ('frank@example.com',   'pro',        'US', true),
    ('grace@example.com',   'free',       'GB', false),
    ('hank@example.com',    'enterprise', 'DE', true),
    ('irene@example.com',   'pro',        'JP', true),
    ('jack@example.com',    'free',       'US', true)
ON CONFLICT DO NOTHING;
