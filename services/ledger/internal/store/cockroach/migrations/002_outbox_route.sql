CREATE TABLE IF NOT EXISTS outbox_routes (
 book_id UUID PRIMARY KEY REFERENCES books(book_id), consumer_id STRING NOT NULL,
 bound_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
