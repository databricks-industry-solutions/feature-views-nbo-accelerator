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

from databricks.sdk.core import Config
from databricks import sql

CATALOG = os.getenv("CATALOG", "fins-industry-solutions")
SCHEMA = os.getenv("SCHEMA", "nbo")
VS_ENDPOINT = os.getenv("VS_ENDPOINT", "nbo-vs-endpoint")
VS_INDEX = os.getenv("VS_INDEX", f"{CATALOG}.{SCHEMA}.offers_index")
RANKER_ENDPOINT = os.getenv("RANKER_ENDPOINT", "nbo-ranker")

# Neutral defaults for the aggregation features that the ranker takes as request
# inputs (see notebook 05 design note — online agg tables aren't auto-looked-up here).
_DEFAULT_AGG = {
    "cust_avg_balance_30d": 25000.0,
    "cust_spend_90d": 5000.0,
    "cust_txn_count_7d": 5.0,
}


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
        self._cfg = Config()
        self._vsc = None  # lazily created; import kept local so app boots without it

    # --- connections -------------------------------------------------------
    def _sql_conn(self):
        return sql.connect(
            server_hostname=self._cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
            credentials_provider=lambda: self._cfg.authenticate,
        )

    def _vs_index(self):
        if self._vsc is None:
            from databricks.vector_search.client import VectorSearchClient
            self._vsc = VectorSearchClient(disable_notice=True)
        return self._vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

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

    # --- two-stage recommend ----------------------------------------------
    def recommend(self, customer: dict, context: str, k: int = 10) -> Recommendation:
        timing = Timing()

        # Stage 1 — candidate retrieval (Vector Search ANN over offer embeddings)
        t0 = time.perf_counter()
        res = self._vs_index().similarity_search(
            query_text=context,
            columns=["offer_id", "product_category", "offer_text"],
            num_results=k,
        )
        candidates = res.get("result", {}).get("data_array", [])
        timing.retrieval_ms = (time.perf_counter() - t0) * 1000

        # Stage 2 — ranking (Model Serving scores customer × candidate offers)
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        recs = [{
            "offer_id": c[0],
            "cust_loyalty_tier": customer["loyalty_tier"],
            "cust_risk_band": customer["risk_band"],
            **_DEFAULT_AGG,
        } for c in candidates]

        t1 = time.perf_counter()
        preds = w.serving_endpoints.query(
            name=RANKER_ENDPOINT, dataframe_records=recs
        ).predictions
        timing.ranking_ms = (time.perf_counter() - t1) * 1000

        offers = [{
            "offer_id": c[0],
            "product_category": c[1],
            "offer_text": c[2],
            "score": float(p),
        } for c, p in zip(candidates, preds)]
        offers.sort(key=lambda o: o["score"], reverse=True)
        return Recommendation(offers=offers, timing=timing)
