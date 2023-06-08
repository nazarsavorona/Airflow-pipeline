CREATE TABLE IF NOT EXISTS exchange_rates
(
    id              SERIAL PRIMARY KEY,
    date            DATE       NOT NULL,
    base_currency   VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    value            DECIMAL    NOT NULL
);