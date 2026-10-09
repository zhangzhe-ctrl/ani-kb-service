"""kb-service orchestration-layer abstract interface contracts (issue-027).

This module declares the product-capability Protocols / ABCs that the
orchestration layer (app/services/) and its collaborators (RagEngineGRPCClient)
must satisfy. Only signatures are defined here — concrete implementations live
in the feature issues
(issue-030 infra, issue-031 retrieve, issue-032 parse, issue-035 query).

References:
- Plan §3.5 (app/services directory)
- Plan step 4 (RetrieveService)
- Plan step 5 (ParseOrchestrator)
- Plan step 8A (QueryOrchestrator)
- Plan §3.4 (RagEngineGRPCClient)
- CLAUDE.md §4.1 (API contract first, then implementation)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol, runtime_checkable


# ── Shared dataclass ────────────────────────────────────────────────────────


@dataclass
class QueryResult:
    """Result of a synchronous Query orchestration (Plan §8A).

    Mirrors the kb_service.proto QueryResponse shape so the gRPC servicer
    can map 1:1 without extra transformation.

    Attributes:
        answer:       LLM-generated answer, or NO_RESULT_ANSWER when any of
                      the three no-result gates trips.
        sources:      Finalized source chunks after dedup + parent backfill.
        session_id:   Session id used for this turn (created or reused).
        input_tokens: Prompt tokens reported by Generate RPC (0 when LLM
                      was not called — gates ① and ②).
        output_tokens: Completion tokens reported by Generate RPC (0 when
                      LLM was not called — gates ① and ②).
    """

    answer: str
    sources: list[dict[str, Any]] = field(default_factory=list)
    session_id: str = ""
    input_tokens: int = 0
    output_tokens: int = 0


# ── RagEngine gRPC client Protocol (Plan §3.4) ──────────────────────────────


@runtime_checkable
class RagEngineClientProtocol(Protocol):
    """Abstract gRPC client surface for rag-engine Parse/Embed/Generate(Stream).

    The concrete RagEngineGRPCClient is implemented in app/rag_engine/client.py.
    """

    async def parse(
        self,
        *,
        download_url: str,
        file_name: str,
        file_type: str,
        chunk_size: int = 1024,
    ) -> list[dict[str, Any]]:
        """Call rag-engine Parse RPC.

        Args:
            download_url: Core API presigned download URL (avoids gRPC 4MB
                          limit; rag-engine downloads the file itself).
            file_name:    Original file name (used for type inference fallback).
            file_type:    pdf | docx | xlsx | pptx | md | txt.
            chunk_size:   Child chunk size override (default 1024).

        Returns:
            List of parsed chunk dicts. Each chunk has keys: chunk_id,
            content, content_type, page_number, parent_content,
            parent_chunk_id, chunk_type, metadata_json, image_bytes
            (optional), image_format (optional).
        """
        ...

    async def embed(
        self, *, texts: list[str], model: str = "", runtime_endpoint: str = ""
    ) -> tuple[list[list[float]], int]:
        """Call rag-engine Embed RPC.

        Args:
            texts: Texts to embed.
            model: Per-KB embedding model name (from the KB row's
                  ``embedding_model`` column); empty uses the rag-engine
                  server default model.
            runtime_endpoint: Cluster runtime endpoint of the KB owner's
                  published inference service; empty falls back to the
                  rag-engine server default endpoint.

        Returns:
            (vectors, dimension) tuple. vectors[i] is the embedding of
            texts[i]; dimension is the embedding dimension (e.g. 1024 for
            bge-m3). The RPC returns a flattened 1-D array; the concrete
            client reshapes it to (count, dimension) before returning.
        """
        ...

    async def generate(
        self,
        *,
        question: str,
        session_id: str,
        context: list[dict[str, Any]],
        history: list[dict[str, str]],
        inference_service_name: str = "",
        max_tokens: int = 2048,
        runtime_endpoint: str = "",
    ) -> dict[str, Any]:
        """Call rag-engine Generate RPC.

        Args:
            question:               User question; appended as the final USER
                                    message after history (reproduces legacy
                                    {query_str} template behavior — the current
                                    turn user appears twice).
            session_id:             Session id (pass-through for token accounting).
            context:                Retrieval sources used to build the RAG
                                    system prompt (DEFAULT_CONTEXT_TEMPLATE).
            history:                Chat history INCLUDING the current-turn
                                    user message (reproduces legacy behavior
                                    where kb-service appends user to Redis
                                    before calling rag-engine).
            inference_service_name: vLLM service name; empty uses the default
                                    model (rag-engine ``settings.vllm_model``).
            max_tokens:             Max completion tokens.

        Returns:
            Dict with keys: answer (str), input_tokens (int),
            output_tokens (int), session_id (str).
        """
        ...

    async def generate_stream(
        self,
        *,
        question: str,
        session_id: str,
        context: list[dict[str, Any]],
        history: list[dict[str, str]],
        inference_service_name: str = "",
        max_tokens: int = 2048,
        runtime_endpoint: str = "",
    ) -> AsyncIterator[dict[str, Any]]:
        """Call rag-engine GenerateStream RPC; async-iterate tokens.

        Declared `async def` to match the concrete implementation pattern
        (async generator: `async def` + `yield`), Plan §3.4, and the sibling
        parse/embed/generate methods. An async generator function is NOT a
        coroutine function, so callers still iterate with
        `async for tok in client.generate_stream(...)` — no preceding
        `await` is needed.

        Yields dict items with keys: content (str), done (bool),
        input_tokens (int), output_tokens (int). The final yielded item has
        done=True and carries the total token usage (vLLM returns usage in
        the last chunk when stream_options.include_usage=True).
        """
        ...


# ── RetrieveService Protocol (Plan step 4) ──────────────────────────────────


@runtime_checkable
class RetrieveServiceProtocol(Protocol):
    """Abstract hybrid retrieval orchestrator (Plan step 4).

    Orchestrates: rag-engine.Embed (query) → direct self-managed Milvus
    vector search (D1, issue-056/#057) → PG keyword search → RRF fusion →
    parent backfill → dedup.
    Result is equivalent to the legacy rag-engine RetrieveService output.
    """

    async def retrieve(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        question: str,
        top_k: int = 5,
        score_threshold: float = 0.3,
        retrieval_mode: str = "hybrid",
        vector_store_id: str | None = None,
        embedding_model: str = "",
        runtime_endpoint: str = "",
    ) -> tuple[list[dict[str, Any]], float]:
        """Run hybrid retrieval and return (sources, max_score).

        Args:
            tenant_id:       Tenant id (explicit filter).
            kb_id:           Knowledge base id.
            question:        User question (embedded for vector search).
            top_k:           Number of sources to return after fusion+dedup.
            score_threshold: Minimum score; gate ② in QueryOrchestrator
                             compares max_score against this value.
            retrieval_mode:  hybrid | vector | keyword.
            vector_store_id: Self-managed Milvus collection name
                             (``kb_{kb_id without dashes}``, D1 issue-056);
                             required for vector/hybrid modes — the
                             implementation raises ValueError if empty
                             (grpc_server resolves it from the KB row).
            embedding_model: Per-KB embedding model name (routed by the
                             rag-engine Embed RPC); empty string uses the
                             rag-engine default model.
            runtime_endpoint: Cluster runtime endpoint of the KB owner's
                             published inference service; empty falls back
                             to the rag-engine server default endpoint.

        Returns:
            (sources, max_score) tuple.
            sources: finalized list of dicts (post dedup + parent backfill),
                     each with keys: chunk_id, doc_id, file_name, page,
                     content, parent_content, parent_chunk_id, chunk_type,
                     score.
            max_score: for hybrid mode = max(vector cosine scores); for
                       vector mode = max(cosine); for keyword mode = max
                       (token coverage). Used by QueryOrchestrator gate ②.
        """
        ...


# ── ParseOrchestrator Protocol (Plan step 5) ────────────────────────────────


@runtime_checkable
class ParseOrchestratorProtocol(Protocol):
    """Abstract document parse pipeline orchestrator (Plan step 5).

    Orchestrates: sign internal-view presigned GET URL → rag-engine.Parse RPC
    → image direct-upload to kb bucket → rag-engine.Embed RPC → self-managed
    Milvus vector insert (D1, issue-056; direct via
    app/vector_store/milvus_client.py, not Core vector-stores) → PG kb_chunks
    write → best-effort summary via Generate RPC → status update.
    """

    async def process_document(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        doc_id: str,
        storage_path: str,
        file_name: str,
        file_type: str,
        chunk_size: int,
        vector_store_id: str,
        embedding_model: str = "",
        embed_runtime_endpoint: str = "",
        generate_runtime_endpoint: str = "",
        run_id: str = "",
    ) -> None:
        """Process a single document end-to-end through the parse pipeline.

        State machine: pending → parsing → indexing → ready | failed.

        Args:
            tenant_id:       Tenant id (explicit filter).
            kb_id:           Knowledge base id.
            doc_id:          Document id (pre-reserved UUID).
            storage_path:    kb-owned object key of the original document
                            (``{kb_id}/{doc_id}``); signed as an internal-view
                            presigned GET URL and handed to rag-engine.Parse.
            file_name:       Original file name.
            file_type:       pdf | docx | xlsx | pptx | md | txt.
            chunk_size:      Child chunk size.
            vector_store_id: Target self-managed Milvus collection name
                            (``kb_{kb_id without dashes}``).
            embedding_model: Per-KB embedding model name (from the KB row's
                            ``embedding_model`` column); empty uses the
                            rag-engine server default.
            embed_runtime_endpoint: Cluster runtime endpoint of the KB owner's
                            published **embedding** inference service (resolved
                            from ``embedding_model``); empty falls back to the
                            rag-engine server default endpoint.
            generate_runtime_endpoint: Cluster runtime endpoint of the KB
                            owner's published **chat** inference service
                            (resolved from the KB's default inference service);
                            used by the best-effort summary Generate RPC;
                            empty falls back to the rag-engine default.

        Returns:
            None. The document's parse_status is updated to `ready` on
            success or `failed` (with sanitized error message) on exception.
        """
        ...


# ── QueryOrchestrator Protocol (Plan step 8A) ──────────────────────────────


@runtime_checkable
class QueryOrchestratorProtocol(Protocol):
    """Abstract synchronous Query orchestrator (Plan step 8A).

    Orchestrates: RetrieveService.retrieve → three no-result gates →
    rag-engine.Generate RPC → return QueryResult.
    """

    async def query(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        question: str,
        session_id: str,
        top_k: int,
        score_threshold: float,
        retrieval_mode: str,
        inference_service_name: str,
        vector_store_id: str,
        history: list[dict[str, str]],
        embed_runtime_endpoint: str = "",
        generate_runtime_endpoint: str = "",
    ) -> QueryResult:
        """Run a synchronous RAG query and return a QueryResult.

        Three no-result gates (reproduces legacy QAService behavior):
          ① retrieval empty → NO_RESULT_ANSWER, tokens=0 (LLM not called).
          ② max_score < score_threshold → NO_RESULT_ANSWER, tokens=0
            (LLM not called).
          ③ sources empty after dedup → NO_RESULT_ANSWER, tokens = LLM
            actual usage (LLM was called).

        Args:
            tenant_id:              Tenant id (explicit filter).
            kb_id:                  Knowledge base id.
            question:               User question.
            session_id:             Session id (empty creates a new one in the
                                    caller; this method receives the resolved
                                    id).
            top_k:                  Override KB default if nonzero.
            score_threshold:        Override KB default if nonzero.
            retrieval_mode:         hybrid | vector | keyword.
            inference_service_name: vLLM service name; empty falls back to
                                    rag-engine default (settings.vllm_model).
            vector_store_id:        Self-managed Milvus collection name
                                    (``kb_{kb_id without dashes}``).
            history:                Chat history INCLUDING the current-turn
                                    user message (reproduces legacy behavior:
                                    kb-service appends user to Redis before
                                    calling rag-engine). Generate RPC appends
                                    `question` as the final USER message,
                                    so the current-turn user appears twice —
                                    this matches the legacy {query_str} template
                                    behavior and is intentional.
            embed_runtime_endpoint: Cluster runtime endpoint of the KB owner's
                                    published **embedding** inference service
                                    (resolved from the KB's ``embedding_model``);
                                    empty falls back to the rag-engine default.
            generate_runtime_endpoint: Cluster runtime endpoint of the KB
                                    owner's published **chat** inference service
                                    (resolved from ``inference_service_name``);
                                    empty falls back to the rag-engine default.

        Returns:
            QueryResult dataclass (answer, sources, session_id, input_tokens,
            output_tokens).
        """
        ...

    async def query_stream(
        self,
        *,
        tenant_id: str,
        kb_id: str,
        question: str,
        session_id: str,
        top_k: int,
        score_threshold: float,
        retrieval_mode: str,
        inference_service_name: str,
        vector_store_id: str,
        history: list[dict[str, str]],
        embed_runtime_endpoint: str = "",
        generate_runtime_endpoint: str = "",
    ) -> AsyncIterator[Any]:
        """Streaming RAG query — async generator (issue-038: Retrieve RPC).

        Same gate logic as ``query()`` but yields typed stream events
        (StreamTokenEvent, StreamSourcesEvent, StreamDoneEvent,
        StreamNoResultEvent) so the Retrieve gRPC handler can map them
        directly to ``RetrieveEvent`` proto messages without duplicating
        the three-gate logic.
        """
        ...
