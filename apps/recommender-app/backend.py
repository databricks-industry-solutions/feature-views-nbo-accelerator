"""Data access layer for the Next-Best-Offer demo app.

Wraps the two-stage recommender: Vector Search retrieval → Model Serving ranking.
All timings are captured per stage so the UI can render a live latency meter and
prove the sub-300ms serving path. Auth uses the SDK Config() (service principal in
the deployed app); resource IDs come from app.yaml `valueFrom` env vars.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks import sql

CATALOG = os.getenv("CATALOG", "fins_industry_solutions")
SCHEMA = os.getenv("SCHEMA", "nbo")
VS_ENDPOINT = os.getenv("VS_ENDPOINT", "nbo-vs-endpoint")
VS_INDEX = os.getenv("VS_INDEX", f"{CATALOG}.{SCHEMA}.offers_index")
# Online-lookup endpoint: fetches the 5 customer features from the online store by
# customer_id at request time (notebook 05b). The request carries only customer_id +
# the offer fields — the "author once, serve online" proof point.
RANKER_ENDPOINT = os.getenv("RANKER_ENDPOINT", "nbo-ranker-online")


@dataclass
class Timing:
    """Per-stage latency in milliseconds for one recommendation request."""
    retrieval_ms: float = 0.0
    ranking_ms: float = 0.0

    @property
    def e2e_ms(self) -> float:
        return self.retrieval_ms + self.ranking_ms


@dataclass
class Recommendation:
    offers: list[dict] = field(default_factory=list)  # ranked, each with score
    timing: Timing = field(default_factory=Timing)


class Backend:
    def __init__(self) -> None:
        # One SDK client, authenticated as the app's service principal (OAuth). Used for
        # embeddings, Vector Search, and the route-optimized ranker — no PAT/SP-secret needed.
        self._w = WorkspaceClient()
        self._cfg = self._w.config

    # --- connections -------------------------------------------------------
    def _sql_conn(self):
        return sql.connect(
            server_hostname=self._cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
            credentials_provider=lambda: self._cfg.authenticate,
        )

    # --- reads -------------------------------------------------------------
    def sample_customers(self, n: int = 25) -> list[dict]:
        q = (f"SELECT customer_id, loyalty_tier, risk_band, annual_income, tenure_months "
             f"FROM `{CATALOG}`.{SCHEMA}.customers LIMIT {int(n)}")
        with self._sql_conn() as conn, conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def latency_results(self) -> list[dict]:
        q = f"SELECT stage, p50, p95, p99 FROM `{CATALOG}`.{SCHEMA}.latency_results"
        with self._sql_conn() as conn, conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def _embed(self, text: str) -> list[float]:
        """Embed the session context client-side so retrieval can use query_vector
        (avoids the per-request server-side embedding hop — ~50ms cheaper)."""
        e = self._w.serving_endpoints.query(
            name="databricks-gte-large-en", input=[text]).data[0]
        return e.embedding if hasattr(e, "embedding") else e["embedding"]

    # --- two-stage recommend ----------------------------------------------
    def recommend(self, customer: dict, context: str, k: int = 10) -> Recommendation:
        timing = Timing()

        # Stage 1 — candidate retrieval (Vector Search ANN over offer embeddings).
        # Query the index through the SDK (auths as the app SP) — no separate VS client / PAT.
        # query_vector avoids the server-side FMAPI embed hop; embed once, up front.
        vec = self._embed(context)
        # Retrieve offer attributes too — the ranker uses base_reward/tier_requirement/category
        # to differentiate offers for a given customer (without them, scores saturate per profile).
        cols = ["offer_id", "product_category", "offer_text", "base_reward", "tier_requirement"]
        t0 = time.perf_counter()
        res = self._w.vector_search_indexes.query_index(
            index_name=VS_INDEX, columns=cols, query_vector=vec, num_results=k,
        )
        candidates = res.result.data_array if res.result else []
        timing.retrieval_ms = (time.perf_counter() - t0) * 1000
        idx = {c: i for i, c in enumerate(cols)}

        # Stage 2 — ranking on the route-optimized online-lookup endpoint. The request carries
        # only customer_id + the offer fields; the endpoint fetches the 5 customer features from
        # the online store by customer_id. Route-optimized → data-plane client (data-plane URL +
        # downscoped OAuth token).
        recs = [{
            "customer_id": customer["customer_id"],
            "offer_id": c[idx["offer_id"]],
            "product_category": c[idx["product_category"]],
            "base_reward": float(c[idx["base_reward"]]),
            "tier_requirement": int(c[idx["tier_requirement"]]),
        } for c in candidates]

        t1 = time.perf_counter()
        preds = self._w.serving_endpoints_data_plane.query(
            name=RANKER_ENDPOINT, dataframe_records=recs
        ).predictions
        timing.ranking_ms = (time.perf_counter() - t1) * 1000

        offers = [{
            "offer_id": c[idx["offer_id"]],
            "product_category": c[idx["product_category"]],
            "offer_text": c[idx["offer_text"]],
            "score": float(p),
        } for c, p in zip(candidates, preds)]
        offers.sort(key=lambda o: o["score"], reverse=True)
        return Recommendation(offers=offers, timing=timing)
