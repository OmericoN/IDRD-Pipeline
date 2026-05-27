# IDRD Pipeline — Academic Approach

## Abstract

This document formalizes the methodological approach of the IDRD Pipeline (Implicit Dataset Reference Detection), a reproducible research pipeline for identifying and characterizing dataset mentions in scientific literature. The approach combines deterministic document processing (metadata retrieval, PDF acquisition, TEI structuring, and section-aware rendering) with schema-constrained LLM extraction to capture both explicit and implicit dataset references.

---

## 1. Research Problem

Dataset usage in scientific writing is frequently under-documented. While some papers provide formal bibliographic references to datasets (explicit reference), many papers describe data resources narratively without a canonical citation (implicit reference). This produces a discoverability gap for meta-research, reproducibility audits, and dataset impact analysis.

The central problem is therefore: **How can we systematically detect and normalize both explicit and implicit dataset references at scale from heterogeneous full-text papers?**

---

## 2. Conceptual Framework

The pipeline operationalizes dataset references as two classes:

1. **Explicit references**: mentions linked to formal citation markers and resolvable bibliography entries.
2. **Implicit references**: semantically identifiable dataset mentions present in narrative text without direct formal citation linkage.

The approach assumes that high-recall implicit detection requires preserving methodological context (e.g., Introduction, Data, Methods) while minimizing low-signal rhetorical sections that can induce false positives (e.g., Related Work, Discussion, Conclusion).

---

## 3. Methodological Design

The system is implemented as a staged, resumable workflow:

1. **Discovery**: Retrieve paper metadata via Semantic Scholar API.
2. **Acquisition**: Download open-access PDFs.
3. **Structuring**: Convert PDF to TEI XML using GROBID.
4. **Representation**: Render TEI into LLM-oriented Markdown.
5. **Extraction**: Apply schema-constrained LLM inference for dataset mention identification.

Pipeline state is persisted in PostgreSQL using per-paper stage flags, enabling interruption-safe continuation and deterministic queueing of incomplete items.

---

## 4. Representation Strategy for LLM Extraction

### 4.1 Section-aware filtering

To improve extraction precision, the renderer excludes sections likely to contain non-local dataset mentions or discourse summaries, including:

- Related Work / Literature Review
- Discussion
- Conclusion
- Acknowledgements
- Funding / declarations / ethics / appendices

Sections such as **Abstract**, **Introduction**, **Methodology/Data**, and retained body content are preserved, as they are most likely to contain operational dataset usage signals.

### 4.2 Citation-grounded rendering

In-text bibliography markers are retained and normalized; references are rendered with metadata (authors, year, title, venue, DOI/URL). Only references cited in retained text are emitted, reducing irrelevant bibliography noise while preserving attribution grounding.

### 4.3 Controlled normalization

The renderer applies conservative cleanup (artifact removal, whitespace normalization, control-character stripping) to reduce parser noise without distorting semantic content.

---

## 5. Extraction Model Assumptions

The extraction phase is designed around the following assumptions:

1. Structured Markdown improves model interpretability versus raw PDF text.
2. Citation-grounded context supports explicit reference resolution.
3. Section-constrained context improves implicit mention precision.
4. Schema-constrained generation reduces hallucination and improves output consistency.

This implies a hybrid architecture: deterministic preprocessing for structure and provenance, probabilistic inference for semantic classification.

---

## 6. Validity and Reliability Considerations

### 6.1 Internal validity

- Stage outputs are deterministic before the LLM stage.
- Queue-based stage execution reduces confounding from partial runs.
- Citation linkage is preserved from body text to references.

### 6.2 Construct validity

- Explicit/implicit categories are operationally defined in extraction schema.
- Section policy aligns observed text with the construct of “methodologically used dataset.”

### 6.3 Reliability

- Resumable processing and persisted stage flags support reproducible runs.
- Per-run logs preserve operational traceability.
- Component outputs are result-object based, improving auditability.

---

## 7. Error Sources and Threats to Inference

1. **PDF-to-XML conversion noise**: malformed structure can distort section boundaries.
2. **Heading variability**: non-standard section titles may evade pattern-based filtering.
3. **Ambiguous mentions**: resource names may overlap with methods, projects, or benchmarks.
4. **Citation incompleteness**: some dataset mentions cannot be resolved to formal references.
5. **Model sensitivity**: extraction quality can vary by model size, prompt design, and chunking.

Mitigation in current design includes conservative cleaning, explicit section policy, citation preservation, and structured output constraints.

---

## 8. Evaluation Perspective

An academically rigorous evaluation should distinguish:

- **Mention detection performance** (precision/recall/F1 for dataset mentions)
- **Reference resolution performance** (correct link from mention to bibliography item/identifier)
- **Type classification performance** (explicit vs implicit)
- **Section attribution accuracy** (where the mention is introduced)

Ground-truth benchmarking on curated papers should be used to estimate error modes and calibrate extraction prompts and section policies.

---

## 9. Reproducibility Protocol

For reproducible experimentation:

1. Fix query parameters and limits.
2. Record API retrieval configuration and timestamps.
3. Preserve run artifacts (`logs/runs/<timestamp>/metadata/`).
4. Use deterministic preprocessing stages unchanged across runs.
5. Evaluate extraction outputs against a fixed annotated benchmark.

This protocol allows fair comparison across model versions, prompt revisions, and section-filter configurations.

---

## 10. Positioning and Contribution

The IDRD approach contributes a practical methodology for dataset-reference mining by:

- coupling a robust scholarly-document processing pipeline with LLM extraction,
- enforcing citation-aware, section-aware document representation,
- and treating implicit dataset references as first-class extraction targets.

Methodologically, the pipeline emphasizes **traceability**, **resumability**, and **representation quality** as prerequisites for reliable large-scale inference in scientific text mining.

