-- init.sql
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    timestamp TIMESTAMP,
    amount DECIMAL,
    currency VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    merchant_name VARCHAR(255),
    payment_method VARCHAR(255),
    order_status VARCHAR(255),
    ip_address VARCHAR(255),
    voucher_code VARCHAR(255),
    affiliateId VARCHAR(255),
    modified_at TIMESTAMP,
    modified_by TEXT,
    change_info JSONB
);

ALTER TABLE
    transactions REPLICA IDENTITY FULL;

CREATE
OR REPLACE FUNCTION record_changed_columns() RETURNS TRIGGER AS $ $ DECLARE change_details JSONB;

BEGIN change_details := '{}' :: JSONB;

-- Initialize an empty JSONB object
-- Check each column for changes and record as necessary
IF NEW.amount IS DISTINCT
FROM
    OLD.amount THEN change_details := jsonb_insert(
        change_details,
        '{amount}',
        jsonb_build_object('old', OLD.amount, 'new', NEW.amount)
    );

END IF;

-- Add user and timestamp
change_details := change_details || jsonb_build_object(
    'modified_by',
    current_user,
    'modified_at',
    now()
);

-- Update the change_info column
NEW.change_info := change_details;

NEW.modified_by := current_user;

NEW.modified_at := CURRENT_TIMESTAMP;

RETURN NEW;

END;

$ $ LANGUAGE plpgsql;

CREATE TRIGGER trigger_record_change_info BEFORE
UPDATE
    ON transactions FOR EACH ROW EXECUTE FUNCTION record_changed_columns();

CREATE
OR REPLACE FUNCTION record_change() RETURNS TRIGGER AS $ $ BEGIN NEW.modified_by := current_user;

NEW.modified_at := CURRENT_TIMESTAMP;

RETURN NEW;

END;

$ $ LANGUAGE plpgsql;

CREATE TRIGGER trigger_record_user_on_update BEFORE
UPDATE
    ON transactions FOR EACH ROW EXECUTE FUNCTION record_change();

CREATE
OR REPLACE FUNCTION cancel_random_transaction() RETURNS VOID AS $ $ DECLARE random_tid VARCHAR;

BEGIN -- Select a random transaction_id excluding those with order_status = 'FAILED'
SELECT
    transaction_id INTO random_tid
FROM
    transactions
WHERE
    order_status <> 'FAILED'
ORDER BY
    RANDOM()
LIMIT
    1;

-- Update the order_status to 'CANCELLED' for the randomly selected transaction
UPDATE
    transactions
SET
    order_status = 'CANCELLED'
WHERE
    transaction_id = random_tid;

-- Optional: Print a message indicating which transaction was cancelled
RAISE NOTICE 'Cancelled transaction with transaction_id: %',
random_tid;

END;

$ $ LANGUAGE plpgsql;