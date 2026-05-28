# Why DataSight

DataSight exists because dataset impact is hard to measure from formal citations alone. Scholarly papers often describe datasets in methods, data, or results sections without citing the dataset as a first-class bibliography item. Those implicit references are valuable evidence for reuse, but they are difficult to find at scale.

## Research Problem

Dataset references usually appear in two forms:

- **Explicit references:** a dataset is tied to a formal citation marker, bibliography entry, DOI, repository page, or data paper.
- **Implicit references:** a dataset is named or described in narrative text without a direct dataset citation.

The second form creates a discoverability gap for research support, reproducibility audits, dataset stewardship, and institutional reporting.

## Core Idea

DataSight uses deterministic document processing before any semantic extraction:

```text
publication search -> PDF download -> GROBID TEI XML -> curated Markdown -> mention detection -> structured extraction -> UM matching
```

The goal is to preserve the context that matters for dataset usage while removing text that is likely to create noise. PDFs are converted to TEI XML, rendered into LLM-friendly Markdown, and then processed through staged detection and matching.

## Why Structured Text Matters

Raw PDFs are inconsistent. They mix columns, footers, references, captions, tables, and body text in ways that make downstream extraction fragile. DataSight converts papers into a cleaner representation so the extractor can reason over:

- title, authors, and abstract
- section-aware body text
- retained citation markers
- bibliography references cited by retained text
- generated artifacts that can be inspected later

This makes the pipeline auditable instead of treating each PDF as an opaque blob.

## Reliability Principles

DataSight is designed around reproducible, inspectable stages:

- **Resumable state:** each stage records durable progress in PostgreSQL.
- **Generated artifact separation:** source inputs live in `data/`; runtime outputs live in `storage/`.
- **Queue-backed execution:** Celery and Redis allow long-running work to be monitored from the API and GUI.
- **Conservative resets:** reset clears generated state without deleting curated reference inputs.
- **Institutional matching:** extracted mentions can be compared against imported Maastricht University dataset metadata.

## Intended Use

DataSight is not just a PDF scraper. It is a research data workflow for finding hidden evidence of dataset reuse, preserving enough provenance to inspect why a mention was detected, and giving operators a way to run the pipeline through an API, CLI, or GUI.
