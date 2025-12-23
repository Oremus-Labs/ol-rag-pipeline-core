-- Phase 2: Postgres metadata contract tables (Section C1 in PLAN.md)

create table if not exists documents (
  document_id text primary key,
  source text not null,
  source_uri text not null,
  canonical_url text,
  title text,
  author text,
  published_year int,
  language text,
  content_type text,
  is_scanned boolean,
  status text not null default 'discovered',
  content_fingerprint text,
  canonical_sha256 text,
  canonical_etag text,
  categories_json jsonb,
  source_dataset text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_documents_source on documents(source);
create index if not exists idx_documents_status on documents(status);

create table if not exists document_files (
  document_id text not null references documents(document_id) on delete cascade,
  variant text not null,
  storage_uri text not null,
  sha256 text,
  bytes bigint,
  mime_type text,
  created_at timestamptz not null default now(),
  primary key (document_id, variant)
);

create table if not exists extractions (
  document_id text not null references documents(document_id) on delete cascade,
  pipeline_version text not null,
  extractor text not null,
  extracted_uri text,
  metrics_json jsonb,
  created_at timestamptz not null default now(),
  primary key (document_id, pipeline_version, extractor)
);

create table if not exists document_categories (
  document_id text not null references documents(document_id) on delete cascade,
  category text not null,
  created_at timestamptz not null default now(),
  primary key (document_id, category)
);

create table if not exists document_search (
  document_id text primary key references documents(document_id) on delete cascade,
  preview_text text not null,
  search_tsv tsvector not null,
  updated_at timestamptz not null default now()
);

create index if not exists idx_document_search_tsv on document_search using gin(search_tsv);

create table if not exists document_links (
  document_id text not null references documents(document_id) on delete cascade,
  link_type text not null,
  url text not null,
  label text,
  created_at timestamptz not null default now(),
  unique (document_id, link_type, url)
);

create table if not exists chunks (
  document_id text not null references documents(document_id) on delete cascade,
  pipeline_version text not null,
  chunk_id text not null,
  chunk_index int not null,
  section_path text,
  token_count int,
  sha256 text,
  text_uri text,
  page_start int,
  page_end int,
  locator text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chunk_id),
  unique (document_id, pipeline_version, chunk_index)
);

create index if not exists idx_chunks_doc_pipeline on chunks(document_id, pipeline_version);

create table if not exists provenance (
  document_id text not null references documents(document_id) on delete cascade,
  pipeline_version text not null,
  source_uri text not null,
  citation_anchor text,
  locator text,
  snippet text,
  created_at timestamptz not null default now(),
  primary key (document_id, pipeline_version, source_uri)
);

create table if not exists processing_runs (
  run_id uuid primary key,
  correlation_id uuid not null,
  pipeline_version text not null,
  document_id text references documents(document_id) on delete set null,
  idempotency_key text,
  status text not null,
  started_at timestamptz,
  finished_at timestamptz,
  metrics_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_processing_runs_doc_latest
  on processing_runs (document_id, pipeline_version, started_at desc nulls last);
create index if not exists idx_processing_runs_corr
  on processing_runs (correlation_id, pipeline_version);

create table if not exists processing_errors (
  error_id uuid primary key,
  run_id uuid references processing_runs(run_id) on delete cascade,
  correlation_id uuid not null,
  pipeline_version text not null,
  document_id text references documents(document_id) on delete set null,
  step text not null,
  error_code text,
  message text not null,
  details_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_processing_errors_corr
  on processing_errors (correlation_id, pipeline_version);

create table if not exists ocr_runs (
  ocr_run_id uuid primary key,
  document_id text not null references documents(document_id) on delete cascade,
  pipeline_version text not null,
  engine text not null,
  status text not null,
  metrics_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_ocr_runs_doc on ocr_runs(document_id, pipeline_version, created_at desc);

create table if not exists ocr_pages (
  ocr_run_id uuid not null references ocr_runs(ocr_run_id) on delete cascade,
  page_number int not null,
  consensus_uri text,
  quality_json jsonb,
  created_at timestamptz not null default now(),
  primary key (ocr_run_id, page_number)
);

create table if not exists review_queue (
  review_id uuid primary key,
  document_id text not null references documents(document_id) on delete cascade,
  pipeline_version text not null,
  reason text not null,
  status text not null default 'open',
  created_at timestamptz not null default now(),
  resolved_at timestamptz
);

