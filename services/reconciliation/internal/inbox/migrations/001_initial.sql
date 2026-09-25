CREATE TABLE IF NOT EXISTS schema_migrations (
 version INT8 PRIMARY KEY, checksum STRING NOT NULL
);
CREATE TABLE IF NOT EXISTS event_inbox (
 book_id UUID NOT NULL, event_id UUID NOT NULL, digest STRING NOT NULL CHECK(length(digest)=64),
 received_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(book_id,event_id)
);
CREATE TABLE IF NOT EXISTS ledger_operations (
 book_id UUID NOT NULL, operation_id UUID NOT NULL, event_id UUID NOT NULL,
 request_hash STRING NOT NULL CHECK(length(request_hash)=64), kind STRING NOT NULL,
 outcome STRING NOT NULL CHECK(outcome IN ('APPLIED','REJECTED')),
 recorded_at TIMESTAMPTZ NOT NULL, receipt JSONB NOT NULL,
 PRIMARY KEY(book_id,operation_id), UNIQUE(book_id,event_id),
 FOREIGN KEY(book_id,event_id) REFERENCES event_inbox(book_id,event_id)
);
