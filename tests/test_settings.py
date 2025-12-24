from ol_rag_pipeline_core.config import Settings


def test_settings_parses_required_fields() -> None:
    settings = Settings.model_validate(
        {
            "PIPELINE_VERSION": "v1",
            "DATASET_VERSION": "2025-12-23",
            "QDRANT_URL": "http://localhost:6333",
            "S3_ENDPOINT": "http://localhost:9000",
            "S3_BUCKET": "rag-artifacts",
            "NATS_URL": "nats://localhost:4222",
        }
    )
    assert settings.pipeline_version == "v1"
