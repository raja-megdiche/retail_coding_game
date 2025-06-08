--Couche silver dans laquelle on va ajouter les primary key et des colonnes supplémentaires
DROP TABLE IF EXISTS silver.clients;
CREATE TABLE silver.clients (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    job TEXT,
    email TEXT,
    account_id BIGINT NOT NULL
);
DROP TABLE IF EXISTS silver.products;
CREATE TABLE silver.products (
    id BIGINT PRIMARY KEY,
    ean BIGINT NOT NULL,
    brand TEXT,
    description TEXT
);
DROP TABLE IF EXISTS silver.stores;
CREATE TABLE silver.stores (
    id BIGINT PRIMARY KEY,
    latlng POINT NOT NULL,
    latitude DOUBLE PRECISION GENERATED ALWAYS AS (latlng[0]) STORED,
    longitude DOUBLE PRECISION GENERATED ALWAYS AS (latlng[1]) STORED,
    opening SMALLINT,
    closing SMALLINT,
    type SMALLINT
);
DROP TABLE IF EXISTS silver.transactions;
CREATE TABLE silver.transactions (
    transaction_id BIGINT PRIMARY KEY,
    client_id BIGINT NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    store_id BIGINT NOT NULL,
    date_created TIMESTAMP GENERATED ALWAYS AS (
        date + make_interval(hours := hour, mins := minute)
    ) STORED,
    account_id BIGINT NOT NULL
);