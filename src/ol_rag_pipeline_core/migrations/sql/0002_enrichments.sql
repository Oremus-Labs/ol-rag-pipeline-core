-- Phase 6/7: Day-2 enrichment history (LLM-assisted metadata improvements)

create table if not exists chunk_enrichments (
  chunk_id text not null references chunks(chunk_id) on delete cascade,
  enrichment_version text not null,
  model text not null,
  chunk_sha256 text not null,
  input_sha256 text not null,
  confidence double precision,
  accepted boolean not null default false,
  output_json jsonb,
  error text,
  applied_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chunk_id, enrichment_version)
);

create index if not exists idx_chunk_enrichments_version on chunk_enrichments(enrichment_version);
create index if not exists idx_chunk_enrichments_accepted on chunk_enrichments(accepted);
