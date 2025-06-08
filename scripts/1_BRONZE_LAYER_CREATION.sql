----Couche bronze
DROP TABLE IF EXISTS bronze.clients;
CREATE TABLE bronze.clients (
    id BIGINT,
    name TEXT,
    job TEXT,
    email TEXT,
    account_id BIGINT
);
DROP TABLE IF EXISTS bronze.products;
CREATE TABLE bronze.products (
    id BIGINT,
    ean BIGINT,
    brand TEXT,
    description TEXT
);
DROP TABLE IF EXISTS bronze.stores;
CREATE TABLE bronze.stores (
    id BIGINT,
    latlng POINT,
    opening SMALLINT,
    closing SMALLINT,
    type SMALLINT
);
DROP TABLE IF EXISTS bronze.transactions;
CREATE TABLE bronze.transactions (
    transaction_id BIGINT,
    client_id BIGINT,
    date DATE,
    hour INTEGER,
    minute INTEGER,
    product_id BIGINT NOT NULL,
    quantity INTEGER,
    store_id BIGINT
);
