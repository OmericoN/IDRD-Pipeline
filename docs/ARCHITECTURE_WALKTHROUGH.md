# Architecture Walkthrough

This codebase is meant to read like a pipeline story, not a pile of entrypoints.

## Request Flow

```text
GUI / CLI / Celery
  -> interfaces
  -> application use case
  -> domain schemas and stage metadata
  -> infrastructure adapters
  -> PostgreSQL, Redis, GROBID, Semantic Scholar, storage/
```

The FastAPI app starts in `idrd.api.main`, which includes the router assembled by `idrd.interfaces.api.router`. Each route module handles one HTTP concern: health, stages, runs, insights, imports, or admin reset.

The CLI starts at `idrd.cli:main`, then delegates to `idrd.interfaces.cli.main`. It shares the same application services as the API instead of maintaining a second pipeline implementation.

## Stage Flow

The canonical stage order lives in `idrd.domain.stages`. The argument and task mapping lives in `idrd.application.stage_registry`, so API, CLI, and Celery all agree on how a stage is described and invoked.

Full runs are coordinated by `idrd.application.orchestrator`. Individual stages live in `idrd.application.pipeline_services`; these functions are intentionally procedural because they describe the pipeline steps directly:

```text
discover -> download_pdf -> grobid_convert -> render_document
  -> detect_mentions -> extract_features -> match_um_dataset -> export_insights
```

## Infrastructure

Infrastructure code owns side effects:

- `infrastructure.persistence` owns SQL and transactions.
- `infrastructure.ingestion` owns PDF, GROBID, and markdown filesystem adapters.
- `infrastructure.pubfetcher` owns external publication search.
- `infrastructure.worker` owns Celery integration.
- `infrastructure.health` owns external readiness checks.

`PipelineRepository` remains the public repository class, but its methods are composed from focused persistence modules so contributors can find run tracking, publication/artifact persistence, mention records, UM matching, and exports separately.

## Compatibility

Older imports under `idrd.pipeline`, `idrd.storage.repository`, `idrd.ingestion`, `idrd.pubfetcher`, `idrd.models.results`, and `idrd.cli` remain as shims for one transition pass. New code should import from `domain`, `application`, `infrastructure`, or `interfaces` directly.
