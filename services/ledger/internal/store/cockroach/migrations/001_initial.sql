CREATE TABLE IF NOT EXISTS schema_migrations (version INT PRIMARY KEY, checksum STRING NOT NULL);
CREATE TABLE IF NOT EXISTS books (
 book_id UUID PRIMARY KEY, entity_id UUID NOT NULL, country STRING NOT NULL CHECK(country='KE'),
 currency STRING NOT NULL CHECK(currency='KES'), scale INT NOT NULL CHECK(scale=2),
 synthetic BOOL NOT NULL CHECK(synthetic), default_cap DECIMAL NOT NULL CHECK (default_cap >= 0 AND default_cap <= 99999999999999999999999999999999999999 AND default_cap = trunc(default_cap))
);
CREATE TABLE IF NOT EXISTS accounts (
 book_id UUID NOT NULL REFERENCES books(book_id), account_id UUID NOT NULL, owner_id UUID NOT NULL,
 purpose STRING NOT NULL CHECK(purpose IN ('WALLET','PROVIDER_POOL','FEE_INCOME')),
 PRIMARY KEY(book_id,account_id), UNIQUE(book_id,account_id,purpose)
);
CREATE TABLE IF NOT EXISTS posting_policies (
 book_id UUID NOT NULL REFERENCES books(book_id), policy_id UUID NOT NULL, fee INT8 NOT NULL CHECK(fee>=0),
 fee_account_id UUID NOT NULL, pool_id UUID NOT NULL, provider_account_id UUID NOT NULL, capability_id UUID NOT NULL,
 max_hold_seconds INT8 NOT NULL CHECK(max_hold_seconds>0 AND max_hold_seconds<=900),
 PRIMARY KEY(book_id,policy_id),
 FOREIGN KEY(book_id,fee_account_id) REFERENCES accounts(book_id,account_id),
 FOREIGN KEY(book_id,pool_id) REFERENCES accounts(book_id,account_id)
);
CREATE TABLE IF NOT EXISTS account_balances (
 book_id UUID NOT NULL, account_id UUID NOT NULL, purpose STRING NOT NULL,
 debits DECIMAL NOT NULL CHECK (debits >= 0 AND debits <= 99999999999999999999999999999999999999 AND debits = trunc(debits)), credits DECIMAL NOT NULL CHECK (credits >= 0 AND credits <= 99999999999999999999999999999999999999 AND credits = trunc(credits)), held DECIMAL NOT NULL CHECK (held >= 0 AND held <= 99999999999999999999999999999999999999 AND held = trunc(held)), version INT8 NOT NULL CHECK(version>0),
 PRIMARY KEY(book_id,account_id),
 FOREIGN KEY(book_id,account_id,purpose) REFERENCES accounts(book_id,account_id,purpose),
 CHECK(CASE WHEN purpose='PROVIDER_POOL' THEN debits-credits>=held ELSE credits-debits>=held END),
 CHECK(purpose!='FEE_INCOME' OR held=0)
);
CREATE TABLE IF NOT EXISTS spending_controls (
 book_id UUID NOT NULL REFERENCES books(book_id), subject_kind STRING NOT NULL CHECK(subject_kind IN ('SUBJECT','ACCOUNT')),
 subject_id UUID NOT NULL, version INT8 NOT NULL CHECK(version>0), epoch INT8 NOT NULL CHECK(epoch>0),
 debit_frozen BOOL NOT NULL, credit_frozen BOOL NOT NULL, daily_cap DECIMAL NOT NULL CHECK (daily_cap >= 0 AND daily_cap <= 99999999999999999999999999999999999999 AND daily_cap = trunc(daily_cap)),
 PRIMARY KEY(book_id,subject_kind,subject_id), CHECK(subject_kind!='ACCOUNT' OR daily_cap=0)
);
CREATE TABLE IF NOT EXISTS business_claims (
 book_id UUID NOT NULL REFERENCES books(book_id), payment_id UUID NOT NULL, stage STRING NOT NULL CHECK(stage='EXECUTION'),
 admission_operation_id UUID NOT NULL, admission_outcome STRING NOT NULL CHECK(admission_outcome IN ('APPLIED','REJECTED')),
 material_hash STRING NOT NULL, state STRING NOT NULL CHECK(state IN ('APPLIED','REJECTED','RESERVED','EXPOSED','CAPTURED','RELEASED')),
 hold_id UUID NULL, result_operation_id UUID NOT NULL, PRIMARY KEY(book_id,payment_id,stage)
);
CREATE TABLE IF NOT EXISTS holds (
 book_id UUID NOT NULL REFERENCES books(book_id), hold_id UUID NOT NULL, payment_id UUID NOT NULL,
 attempt_id UUID NOT NULL, terms JSONB NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
 state STRING NOT NULL CHECK(state IN ('RESERVED','EXPOSED','CAPTURED','RELEASED')),
 version INT8 NOT NULL CHECK(version>0), PRIMARY KEY(book_id,hold_id), UNIQUE(book_id,attempt_id), UNIQUE(book_id,payment_id)
);
CREATE TABLE IF NOT EXISTS limit_usage (
 book_id UUID NOT NULL REFERENCES books(book_id), owner_id UUID NOT NULL, bucket DATE NOT NULL,
 reserved DECIMAL NOT NULL CHECK (reserved >= 0 AND reserved <= 99999999999999999999999999999999999999 AND reserved = trunc(reserved)), consumed DECIMAL NOT NULL CHECK (consumed >= 0 AND consumed <= 99999999999999999999999999999999999999 AND consumed = trunc(consumed)), version INT8 NOT NULL CHECK(version>0),
 PRIMARY KEY(book_id,owner_id,bucket)
);
CREATE TABLE IF NOT EXISTS journals (
 book_id UUID NOT NULL REFERENCES books(book_id), journal_id UUID NOT NULL, operation_id UUID NOT NULL,
 template STRING NOT NULL, recorded_at TIMESTAMPTZ NOT NULL,
 line_count INT NOT NULL CHECK(line_count BETWEEN 2 AND 8), digest STRING NOT NULL,
 PRIMARY KEY(book_id,journal_id), UNIQUE(book_id,operation_id)
);
CREATE TABLE IF NOT EXISTS journal_lines (
 book_id UUID NOT NULL, journal_id UUID NOT NULL, ordinal INT NOT NULL CHECK(ordinal BETWEEN 0 AND 7),
 account_id UUID NOT NULL, side STRING NOT NULL CHECK(side IN ('DEBIT','CREDIT')), units INT8 NOT NULL CHECK(units>0),
 PRIMARY KEY(book_id,journal_id,ordinal), UNIQUE(book_id,journal_id,account_id),
 FOREIGN KEY(book_id,journal_id) REFERENCES journals(book_id,journal_id),
 FOREIGN KEY(book_id,account_id) REFERENCES accounts(book_id,account_id)
);
CREATE TABLE IF NOT EXISTS account_events (
 book_id UUID NOT NULL, account_id UUID NOT NULL, version INT8 NOT NULL CHECK(version>0),
 operation_id UUID NOT NULL, body JSONB NOT NULL, PRIMARY KEY(book_id,account_id,version),
 FOREIGN KEY(book_id,account_id) REFERENCES accounts(book_id,account_id)
);
CREATE TABLE IF NOT EXISTS control_events (
 book_id UUID NOT NULL REFERENCES books(book_id), subject_kind STRING NOT NULL, subject_id UUID NOT NULL,
 version INT8 NOT NULL CHECK(version>0), operation_id UUID NOT NULL, body JSONB NOT NULL,
 PRIMARY KEY(book_id,subject_kind,subject_id,version)
);
CREATE TABLE IF NOT EXISTS hold_events (
 book_id UUID NOT NULL, hold_id UUID NOT NULL, version INT8 NOT NULL CHECK(version>0),
 operation_id UUID NOT NULL, body JSONB NOT NULL, PRIMARY KEY(book_id,hold_id,version),
 FOREIGN KEY(book_id,hold_id) REFERENCES holds(book_id,hold_id)
);
CREATE TABLE IF NOT EXISTS limit_events (
 book_id UUID NOT NULL REFERENCES books(book_id), owner_id UUID NOT NULL, bucket DATE NOT NULL,
 version INT8 NOT NULL CHECK(version>0), operation_id UUID NOT NULL, body JSONB NOT NULL,
 PRIMARY KEY(book_id,owner_id,bucket,version)
);
CREATE TABLE IF NOT EXISTS resolution_evidence (
 book_id UUID NOT NULL REFERENCES books(book_id), evidence_id UUID NOT NULL, digest STRING NOT NULL, body JSONB NOT NULL,
 PRIMARY KEY(book_id,evidence_id)
);
CREATE TABLE IF NOT EXISTS financial_operations (
 book_id UUID NOT NULL REFERENCES books(book_id), operation_id UUID NOT NULL, canonical BYTES NOT NULL,
 request_hash STRING NOT NULL, outcome STRING NOT NULL CHECK(outcome IN ('APPLIED','REJECTED')),
 receipt JSONB NOT NULL, PRIMARY KEY(book_id,operation_id)
);
CREATE TABLE IF NOT EXISTS outbox_facts (
 book_id UUID NOT NULL, event_id UUID NOT NULL, operation_id UUID NOT NULL,
 event_type STRING NOT NULL CHECK(event_type='LedgerOperationResolved'), body JSONB NOT NULL,
 PRIMARY KEY(book_id,event_id), UNIQUE(book_id,operation_id),
 FOREIGN KEY(book_id,operation_id) REFERENCES financial_operations(book_id,operation_id)
);
CREATE TABLE IF NOT EXISTS outbox_delivery (
 book_id UUID NOT NULL, event_id UUID NOT NULL, delivered BOOL NOT NULL DEFAULT false,
 lease_token UUID NULL, lease_until TIMESTAMPTZ NULL, attempts INT8 NOT NULL DEFAULT 0 CHECK(attempts>=0),
 PRIMARY KEY(book_id,event_id), FOREIGN KEY(book_id,event_id) REFERENCES outbox_facts(book_id,event_id)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox_delivery(delivered,lease_until) WHERE delivered=false;
