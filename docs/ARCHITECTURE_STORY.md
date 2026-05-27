# IDRD Pipeline Architecture Story

> Historical note: this long-form architecture narrative predates the active-only cleanup. For current commands and the active code path, use `README.md` and `docs/SETUP_AND_CLI_GUIDE.md`.

This document explains the IDRD Pipeline as a full story: why the project exists, what problem it solves, how the codebase is organized, why the architecture uses certain technologies, and how data flows through the system from publication discovery to UM dataset matching.

It is written for three purposes:

- to help you understand the codebase deeply
- to help you explain the project clearly to supervisors, library colleagues, and technical reviewers
- to act as a technical map for future implementation work

## 1. The Project In One Sentence

The IDRD Pipeline discovers open-access scholarly publications, turns their PDFs into structured text, detects formal and informal dataset mentions, extracts dataset metadata, and checks whether those datasets are affiliated with Maastricht University.

The central idea is simple:

> Datasets are research outputs, but publications often mention them badly. This pipeline tries to recover those hidden dataset footprints from full-text publications.

## 2. The Problem Story

Maastricht University Library wants better insight into dataset research output. Researchers create, curate, and maintain datasets, but those datasets are often reused in publications without being properly cited.

This creates several problems:

- dataset creators do not receive proper recognition
- the university cannot easily monitor the impact of its dataset outputs
- existing citation indexes miss informal or narrative dataset references
- datasets may be mentioned through transitive links, such as data papers or project websites, rather than direct repository records
- important metadata such as DOI, version, access date, repository URL, and creator information may be scattered across text, footnotes, tables, and references

Traditional bibliometric systems are strong at tracking publication citations. They are much weaker at tracking dataset mentions, especially when the dataset is described in ordinary prose rather than cited as a clean bibliographic object.

That is why this project needs full-text processing.

## 3. The Research Insight Behind The Pipeline

The project starts from the observation that dataset references are not always formal. A dataset can appear as:

- a formal citation in the bibliography
- a DOI or repository link
- a data paper that indirectly points to the actual dataset
- a footnote URL
- a project acronym in the methods section
- a sentence such as "we collected survey responses from 500 participants"
- a data availability statement
- a table caption or supplementary material note

The important design choice is that the pipeline does not only look at references. It also reads the narrative structure of the paper.

Your initial research found that descriptive dataset mentions often cluster in methodology, materials, data source, cohort, or data availability sections. The pipeline therefore treats the structure of the paper as meaningful evidence.

## 4. The Current Architecture At A Glance

The canonical pipeline stages are:

```text
discover
  -> download_pdf
  -> grobid_convert
  -> render_document
  -> detect_mentions
  -> extract_features
  -> match_um_dataset
  -> export_insights
```

Each stage has one clear responsibility. This is important because the pipeline is expected to process large batches. Some stages are network-bound, some are CPU-bound, some require Docker/GROBID, and some may call an LLM. Separating them lets the system retry, resume, parallelize, and inspect failures stage by stage.

## 5. Why A Queue-Based Architecture

The old prototype was a monolithic command-line orchestrator. That was useful for early experimentation because everything happened in one process:

1. fetch papers
2. download PDFs
3. convert PDFs
4. render Markdown

For high-throughput work, that shape becomes limiting.

A queue-backed architecture is better because:

- each publication can move through the pipeline independently
- failed tasks can be retried without restarting the whole run
- slow stages can have more workers
- rate-limited stages can be throttled
- GROBID conversion can be isolated from LLM extraction
- future production deployment becomes easier
- stage status can be stored and inspected in PostgreSQL

This is why the new implementation introduces Celery and Redis.

Redis is the message broker. Celery workers consume tasks from Redis and execute pipeline stages. PostgreSQL remains the durable system of record.

## 6. Why This Tech Stack

The project deliberately uses free/open-source infrastructure where possible.

| Requirement | Choice | Reason |
|---|---|---|
| Durable relational state | PostgreSQL | Mature, reliable, open-source, widely supported |
| Vector search | pgvector | Keeps semantic matching inside PostgreSQL instead of paying for Pinecone |
| Queue runtime | Celery + Redis | Proven Python queue stack, easy local setup |
| PDF parsing | GROBID | Strong open-source tool for scholarly PDFs and TEI XML |
| Migrations | Alembic | Professional database schema evolution |
| Validation | Pydantic | Typed schemas for extraction and matching outputs |
| CLI | argparse | Simple standard-library interface |
| Tests | pytest | Lightweight and already used in the repo |
| LLM | Pluggable provider | Allows OpenAI, Groq, Ollama, or another OpenAI-compatible endpoint |

The main architectural rule is:

> Do not outsource core storage or search if PostgreSQL can handle it.

That is why `pgvector` is used instead of Pinecone. Dataset matching needs vector similarity, but the vectors can live in Postgres alongside the rest of the metadata.

## 7. Codebase Map

The current repo has two layers:

- legacy prototype modules that already perform useful ingestion work
- new production-facing modules that define the queue-backed architecture

### 7.1 Production-Facing Pipeline Modules

`src/pipeline/stages.py`

Defines the canonical stage names and stage order. This file is intentionally small but important because stage names should not be duplicated as random strings throughout the codebase.

`src/pipeline/celery_app.py`

Creates the Celery application. It reads broker/backend settings from `src/config.py`, points Celery at `pipeline.tasks`, and sets worker behavior such as JSON serialization and late acknowledgements.

`src/pipeline/tasks.py`

Contains Celery task entrypoints. These functions are thin wrappers around service functions. They are responsible for queue execution, retries, and task names.

`src/pipeline/services.py`

Contains synchronous stage implementations. These functions can be called by Celery workers or by `run-local` CLI commands. This separation is useful because it keeps the business logic independent from the queue runtime.

`src/pipeline/repository.py`

Implements database access for the new canonical Alembic schema. It stores and retrieves publications, artifacts, and stage outputs using the new table layout.

`src/pipeline/schemas.py`

Defines Pydantic models for publications, dataset mentions, extraction evidence, dataset metadata, provenance, UM records, and match decisions. This is the professionalized version of the original extraction taxonomy.

`src/pipeline/candidate_detection.py`

Implements cheap first-pass dataset mention detection. It looks for likely dataset sentences in Markdown and returns candidates for later extraction. This stage is intentionally high recall.

### 7.2 Matching Modules

`src/matching/normalization.py`

Contains text and identifier normalization helpers. These are important because dataset names and DOIs can be written in many slightly different ways.

`src/matching/um_matcher.py`

Implements deterministic UM dataset matching. It currently supports exact DOI/URL matching and metadata similarity using title, aliases, creators, and year. Later, `pgvector` similarity and LLM adjudication can be added around the same decision model.

### 7.3 Database Migration Modules

`alembic.ini`

Alembic configuration file.

`migrations/env.py`

Alembic runtime environment. It loads the project config and uses `POSTGRES_URI`.

`migrations/versions/20260526_0001_open_source_pipeline_schema.py`

Creates the first canonical schema. It enables `pgvector` and creates the main tables.

### 7.4 Legacy But Still Useful Modules

`src/pubfetcher/client.py`

Semantic Scholar API client. It can search papers, request metadata, handle pagination, and apply open-access filters.

`src/ingestion/downloader.py`

Downloads PDFs from open-access URLs. It validates content type and checks that downloaded files begin with the PDF header.

`src/ingestion/converter.py`

Runs GROBID through Docker and converts PDFs into TEI XML.

`src/ingestion/renderer.py`

Converts TEI XML into Markdown. It preserves important paper structure such as headings, abstracts, body sections, references, and footnotes.

These modules are still valuable. The new architecture wraps them rather than throwing them away.

### 7.5 Legacy Cleanup Zone

`src/db/db.py`

This file is a legacy database manager with schema drift. It uses old column naming patterns such as `"paperId"` in some places while the new schema uses canonical `paper_id`. It also contains older experimental tables and methods.

The new queue-backed path does not depend on it. The plan should be to gradually replace it or retire it once all production paths use `PipelineRepository`.

`src/extraction/extractor.py` and `src/extraction/extractor-langextract.py`

These are experimental extraction scripts. They helped explore LLM extraction, but production extraction should move into provider-agnostic pipeline services with explicit schemas, prompt versions, raw response logging, and validation.

## 8. Database Story

PostgreSQL is the central memory of the pipeline. The filesystem stores heavy artifacts such as PDFs, XML files, and Markdown files, but PostgreSQL stores the facts about them.

The first Alembic migration creates these core tables:

### 8.1 `publications`

Stores one row per discovered publication.

Important fields:

- `paper_id`
- `doi`
- `title`
- `abstract`
- `year`
- `source`
- `source_url`
- `open_access_url`
- `raw`

The `raw` JSONB field stores the original API response so the pipeline does not throw away source metadata.

### 8.2 `pipeline_runs`

Stores batch-level runs. This is where a discovery query or run configuration can be tracked.

### 8.3 `stage_runs`

Stores the status of stage execution. This is the table that makes resumability and observability possible.

Example statuses:

- `queued`
- `started`
- `successful`
- `failed`
- `skipped`

### 8.4 `artifacts`

Stores paths and metadata for files generated by the pipeline.

Artifact types include:

- `pdf`
- `tei_xml`
- `markdown`

The files themselves live on disk for now, while the database stores where they are and which publication they belong to.

### 8.5 `document_sections`

Stores structured sections extracted from papers. This table is designed for later semantic search and RAG. It includes an embedding column using `pgvector`.

### 8.6 `dataset_mentions`

Stores extracted dataset mentions. This is where the pipeline records that a publication appears to use, create, cite, or discuss a dataset.

It has JSONB fields for:

- evidence
- metadata
- provenance

It also has a vector column for semantic matching.

### 8.7 `um_datasets`

Stores UM dataset metadata once the university data source is connected.

Expected metadata:

- UM dataset ID
- title
- aliases
- creators
- DOI
- URL
- year
- repository
- keywords
- embedding

### 8.8 `um_match_decisions`

Stores match decisions between extracted dataset mentions and UM dataset records.

This table supports both automatic matching and later human review.

## 9. The Stage-By-Stage Story

This section explains what happens as a publication moves through the system.

### 9.1 Stage 1: `discover`

Goal:

Find candidate scholarly publications that might contain dataset reuse mentions.

Current implementation:

- uses Semantic Scholar through `SemanticScholarClient`
- stores publication metadata through `PipelineRepository.upsert_publications`

Future direction:

- make OpenAlex the primary high-recall discovery source
- use Semantic Scholar for enrichment or fallback
- add UM author heuristics, institution filters, concepts, dataset title searches, and citation expansion

Architecture choice:

Discovery is high recall. It is better to fetch a broader candidate pool and filter later than to miss dataset mentions early.

### 9.2 Stage 2: `download_pdf`

Goal:

Download open-access PDFs for publications that have a PDF URL.

Current implementation:

- `PDFDownloader` downloads and validates PDFs
- `PipelineRepository.persist_download_results` records successful PDF artifacts

Why this matters:

The full-text PDF is the raw source for narrative dataset mentions. Abstract metadata alone is not enough.

Failure cases:

- URL is missing
- URL returns 403 or 404
- content is not a PDF
- download times out
- downloaded file is corrupt

These failures should be recorded and retried carefully, not silently ignored.

### 9.3 Stage 3: `grobid_convert`

Goal:

Convert PDFs into TEI XML.

Current implementation:

- `GrobidConverter` starts or reuses a Dockerized GROBID server
- PDFs are sent to GROBID
- TEI XML files are saved locally
- XML artifact paths are stored in PostgreSQL

Why GROBID:

PDFs are visually structured but not semantically structured. GROBID extracts scholarly structure such as title, authors, abstract, body sections, references, and footnotes.

This stage is the bridge from unstructured PDF to machine-readable academic document.

### 9.4 Stage 4: `render_document`

Goal:

Convert TEI XML into Markdown that is easier for humans and LLMs to read.

Current implementation:

- `renderer.py` parses TEI XML
- extracts title, authors, abstract, body sections, footnotes, and cited references
- excludes low-value sections such as related work, discussion, conclusion, and acknowledgements where appropriate

Why Markdown:

Markdown is simpler for downstream LLM extraction. It preserves enough structure without overwhelming the model with raw XML tags.

Future improvement:

In addition to Markdown, the renderer should persist structured section JSON into `document_sections`, including offsets, section labels, citation markers, and bibliography links.

### 9.5 Stage 5: `detect_mentions`

Goal:

Cheaply detect likely dataset mention candidates before using an LLM.

Current implementation:

- scans Markdown with high-recall rules
- looks for words such as dataset, database, registry, repository, data source
- detects named datasets such as "UK Biobank dataset"
- detects implicit data descriptions such as "we collected survey responses"
- assigns a rough score
- labels likely section type

Why this exists:

LLM calls are expensive and slower than rules. The detector reduces the text that needs to go to the LLM.

Architecture choice:

The detector should prefer recall over precision. False positives can be filtered later. False negatives are more damaging because missed mentions never reach extraction.

### 9.6 Stage 6: `extract_features`

Goal:

Turn candidates into structured dataset mention records.

Current implementation:

- promotes rule candidates into the v1 `DatasetMention` schema
- records evidence text, section, offsets, and confidence

Future implementation:

This stage should call an LLM on selected text windows and produce validated JSON. It should:

- use a provider-agnostic client
- support OpenAI, Groq, Ollama, or other compatible endpoints
- store prompt version
- store model name
- store raw response
- validate output with Pydantic
- consolidate duplicate mentions within the same paper

### 9.7 Stage 7: `match_um_dataset`

Goal:

Decide whether an extracted dataset mention refers to a dataset affiliated with Maastricht University.

Current implementation:

`um_matcher.py` performs deterministic matching:

1. exact DOI/PID match
2. exact URL match
3. title or alias similarity
4. creator similarity
5. year agreement

Future implementation:

Add `pgvector` matching:

- embed UM dataset metadata
- embed extracted mention evidence
- retrieve nearest UM dataset candidates inside PostgreSQL
- combine vector similarity with metadata similarity

LLM adjudication should only be used for ambiguous cases. The deterministic matching should do as much work as possible first.

### 9.8 Stage 8: `export_insights`

Goal:

Export useful results for analysis, review, reporting, or integration with library systems.

Current implementation:

- writes CSV rows from supplied payloads

Future direction:

Exports should include:

- all dataset mentions
- only UM matches
- possible matches needing review
- per-publication extraction confidence
- dataset reuse by faculty, year, field, or repository
- candidates suitable for CRIS enrichment

## 10. The Extraction Schema Story

The original taxonomy had publication-level labels and dataset-level labels. The new schema preserves that logic but groups the fields into more maintainable objects.

### 10.1 `PublicationRecord`

Represents the paper being analyzed.

This is the source context for every dataset mention.

### 10.2 `DatasetMention`

Represents one dataset mention extracted from a publication.

Core fields:

- `publication_id`
- `dataset_name`
- `aliases`
- `dataset_role`
- `reference_directness`
- `evidence`
- `metadata`
- `provenance`

### 10.3 `MentionEvidence`

Stores textual proof.

Examples:

- abstract quote
- body quote
- section heading
- standardized section
- placement type
- citation marker
- bibliography entry

This is important because every extraction should be auditable. A human reviewer should be able to ask, "Why did the pipeline think this is a dataset?" and see the exact evidence.

### 10.4 `DatasetMetadata`

Stores metadata about the dataset itself.

Examples:

- reference title
- DOI or persistent identifier
- dataset authors
- dataset year
- dataset URL
- reference material
- version
- access date

### 10.5 `ExtractionProvenance`

Stores how the extraction was produced.

Examples:

- source TEI path
- source Markdown path
- section ID
- character offsets
- model name
- prompt version
- confidence

This matters for reproducibility. If a model changes, prompt changes, or rendering changes, the pipeline can explain where older results came from.

### 10.6 `UMDatasetRecord`

Represents a dataset from the UM metadata source.

This schema is intentionally adapter-friendly. Later, UM colleagues can provide data through a database, CSV export, API, or internal system. As long as it can be converted into `UMDatasetRecord`, the matching layer does not need to know where it came from.

### 10.7 `UMMatchDecision`

Represents the output of UM matching.

It records:

- match status
- matched UM dataset ID
- matching method
- score
- matched fields
- review requirement

## 11. Why High Recall First

The system is designed to discover broadly and filter later.

Reason:

The cost of missing a dataset mention is high. If discovery is too strict, the publication never enters the pipeline. No later model or matcher can recover it.

The preferred strategy is:

1. collect broad candidate publications
2. download only what is legally/openly available
3. parse structure
4. identify likely dataset windows
5. use LLMs only where useful
6. match UM affiliation carefully

This is a common information retrieval pattern:

> Use cheap broad filters first, then expensive precise filters later.

## 12. Why `pgvector` Instead Of Pinecone

The project needs semantic similarity for matching narrative mentions to UM dataset metadata. A mention might say:

> "We used the Maastricht Aging Study cohort"

while the UM database might store:

> "Maastricht Study on Ageing and Cognitive Health"

Exact string matching may fail. Embeddings can help.

But using Pinecone or another managed vector database would add cost and infrastructure complexity. `pgvector` lets the project store embeddings in PostgreSQL and query nearest neighbors directly.

Benefits:

- no separate paid vector database
- one database backup story
- SQL joins between vectors and metadata
- easier local development
- easier institutional deployment

## 13. Why Local Filesystem First

PDFs, XML files, and Markdown files are large artifacts. Storing them directly in PostgreSQL is usually not ideal.

The current design stores files on disk and records paths in the `artifacts` table.

This is enough for local-server development.

Later, if the project needs object-storage semantics, MinIO can be introduced. MinIO is open-source and S3-compatible, so it would keep the architecture free/open-source.

## 14. CLI Story

The new CLI is intentionally thin.

Main commands:

```bash
uv run src/main.py stages
```

Prints the canonical stage order.

```bash
uv run src/main.py worker-command
```

Prints the Celery worker command.

```bash
uv run src/main.py enqueue discover --query "Maastricht dataset reuse" --limit 100
```

Enqueues a stage for worker execution.

```bash
uv run src/main.py run-local detect_mentions --limit 20
```

Runs a stage synchronously in the current process.

This distinction is useful:

- `enqueue` is for normal queue-backed operation
- `run-local` is for debugging, development, and tests

## 15. How To Explain The Architecture Out Loud

Here is a concise story you can tell:

> The project addresses a gap in research output monitoring: datasets created or affiliated with Maastricht University are often reused in papers but not formally cited. Traditional citation databases miss those informal mentions, so we need to inspect full-text publications.

> The pipeline starts by discovering open-access publications through scholarly APIs. It downloads PDFs, converts them with GROBID into TEI XML, and renders that structure into Markdown and eventually section-level records. Then it performs a two-pass extraction: first a cheap high-recall detector finds likely dataset mention windows, then a schema-driven extraction stage turns those windows into structured dataset metadata.

> After extraction, the system checks whether the dataset might belong to UM. It first uses deterministic matching such as DOI, URL, title, aliases, creators, and year. For harder cases, it is designed to use semantic similarity through pgvector inside PostgreSQL. Only ambiguous cases should go to an LLM or human review.

> Architecturally, the system uses PostgreSQL as the source of truth, pgvector for semantic search, Celery and Redis for high-throughput task execution, GROBID for scholarly PDF parsing, and Pydantic for validated extraction schemas. This keeps the core stack free and open-source while allowing LLM providers to remain pluggable.

## 16. Current Implementation Status

Implemented:

- canonical stage names
- thin CLI
- Celery app and task wrappers
- synchronous service functions
- Alembic migration with PostgreSQL and `pgvector`
- canonical repository for new schema
- Pydantic extraction and matching schemas
- high-recall rule-based candidate detector
- deterministic UM matcher
- tests for detector, matcher, CLI, runtime monitor, and legacy helper behavior

Still prototype or partial:

- OpenAlex primary discovery adapter
- LLM extraction client
- section-level TEI JSON persistence
- embedding generation
- `pgvector` nearest-neighbor matching service
- human review workflow
- production monitoring dashboards
- full retirement of legacy `src/db/db.py`

## 17. Known Technical Debt

The biggest technical debt is the coexistence of old and new database layers.

Old layer:

- `src/db/db.py`
- uses legacy naming and older schema assumptions
- currently still has type-checking issues
- useful for older tests and prototype behavior

New layer:

- `src/pipeline/repository.py`
- uses the Alembic schema
- canonical names such as `paper_id`
- intended production path

Recommended direction:

1. keep legacy tests passing
2. move all production pipeline paths to `PipelineRepository`
3. add integration tests against a disposable Postgres database
4. retire or drastically shrink `src/db/db.py`

## 18. Testing Story

Current test command:

```bash
uv run pytest -q
```

Current coverage includes:

- stage helper behavior
- candidate detection
- UM matching
- CLI parsing
- runtime monitor state
- legacy database helper behavior with fake cursors

Important future tests:

- migration test against real PostgreSQL with `pgvector`
- GROBID conversion fixture test
- renderer golden-file tests
- LLM extraction contract tests with mocked responses
- end-to-end local smoke test with a small ground-truth corpus
- throughput test with many metadata records

## 19. What Happens When UM Provides Dataset Metadata

When UM colleagues provide access to dataset metadata, the clean integration point is `UMDatasetRecord`.

The adapter should convert UM records into:

- `um_dataset_id`
- title
- aliases
- creators
- DOI
- URL
- year
- repository
- keywords
- raw source metadata

Then the records can be inserted into `um_datasets`.

After embeddings are added, each UM dataset will also have an embedding vector. Extracted mentions will have embeddings too. Matching can then combine:

- exact identifiers
- lexical similarity
- creator/year evidence
- vector similarity
- LLM adjudication for ambiguous cases

## 20. What Happens When LLM Extraction Is Added

The current `extract_features` stage is rule-based scaffolding. The future LLM version should work like this:

1. receive mention candidates from `detect_mentions`
2. expand each candidate into a context window
3. include relevant references or footnotes if available
4. call a provider-agnostic LLM client
5. validate the response against `DatasetMention`
6. store raw model response for audit
7. store validated extraction
8. consolidate duplicates per publication

The LLM should not receive the entire paper by default. It should receive selected windows. This reduces cost and improves focus.

## 21. Main Design Principles

The codebase should continue following these principles:

- keep infrastructure free/open-source where possible
- store durable state in PostgreSQL
- keep semantic search in `pgvector`
- make each stage idempotent and retryable
- preserve raw source metadata
- preserve exact evidence quotes
- validate all extraction outputs
- use LLMs selectively, not everywhere
- make UM integration adapter-based
- make every automated match explainable

## 22. Mental Model For The Whole System

Think of the system as four layers:

### Layer 1: Acquisition

Find publications and get PDFs.

Stages:

- `discover`
- `download_pdf`

### Layer 2: Document Understanding

Turn PDFs into structured text.

Stages:

- `grobid_convert`
- `render_document`

### Layer 3: Dataset Understanding

Find and extract dataset mentions.

Stages:

- `detect_mentions`
- `extract_features`

### Layer 4: Institutional Insight

Connect extracted mentions to UM datasets and export results.

Stages:

- `match_um_dataset`
- `export_insights`

That is the whole project in one pipeline:

```text
publications -> full text -> structured document -> dataset mentions -> UM matches -> insights
```

## 23. The Story In One Paragraph

The IDRD Pipeline is a high-throughput, open-source system for recovering hidden dataset references from scholarly literature. It discovers candidate publications, downloads open-access PDFs, uses GROBID to convert them into structured TEI XML, renders them into LLM-friendly Markdown, detects likely dataset mentions, extracts structured dataset metadata, and matches those mentions against Maastricht University dataset records. PostgreSQL is the source of truth, `pgvector` handles semantic similarity, Celery and Redis provide scalable task execution, and Pydantic ensures extraction outputs are validated. The architecture is designed to start locally, remain low-cost, and grow toward a professional library workflow for monitoring dataset research output.
