CREATE TABLE payment_message
(
    id             SERIAL PRIMARY KEY,
    amount         DOUBLE PRECISION NOT NULL,
    correlation_id VARCHAR(255)     NOT NULL
);


CREATE TABLE payment_transaction
(
    id           SERIAL PRIMARY KEY,
    amount       DOUBLE PRECISION NOT NULL,
    processor_id INTEGER          NOT NULL,
    timestamp    TIMESTAMPTZ      NOT NULL
);

CREATE INDEX idx_payment_transaction_processor_id ON payment_transaction (processor_id, timestamp);