"""Data access layer for the Next-Best-Offer demo app.

Rank-all recommender: score the FULL offer catalog through the online-lookup
Model Serving endpoint. The request carries only `{customer_id, offer fields}`;
the route-optimized `nbo-ranker-online` endpoint fetches the 5 customer features
from the online store by `customer_id` at request time (the "author once, serve
online" proof point) and returns an acceptance probability per offer. There is no
retrieval stage — for a catalog this size you score everything directly, which
mirrors the feature-serving benchmark exactly.

Timing is captured for the single feature-read+rank path so the UI can render a
live latency meter and make the sub-300ms serving claim tangible. Auth uses the
SDK Config (service principal in the deployed app); resource IDs come from
app.yaml `valueFrom` env vars.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks import sql

CATALOG = os.getenv("CATALOG", "fins_industry_solutions")
SCHEMA = os.getenv("SCHEMA", "nbo")
# Online-lookup endpoint: fetches the 5 customer features from the online store by
# customer_id at request time (notebook 05). The request carries only customer_id +
# the offer fields — the "author once, serve online" proof point.
RANKER_ENDPOINT = os.getenv("RANKER_ENDPOINT", "nbo-ranker-online")


@dataclass
class Timing:
    """Feature-read + rank latency in milliseconds for one recommendation request."""
    rank_ms: float = 0.0

    @property
    def e2e_ms(self) -> float:
        return self.rank_ms


@dataclass
class Recommendation:
    offers: list[dict] = field(default_factory=list)  # ranked, each with score
    timing: Timing = field(default_factory=Timing)


class Backend:
    def __init__(self) -> None:
        # One SDK client, authenticated as the app's service principal (OAuth). Used for
        # the route-optimized ranker and SQL reads — no PAT/SP-secret needed.
        self._w = WorkspaceClient()
        self._cfg = self._w.config

    # --- connections -------------------------------------------------------
    def _warehouse_id(self) -> str:
        wid = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not wid:
            raise RuntimeError(
                "DATABRICKS_WAREHOUSE_ID is not set. Add the 'sql-warehouse' resource "
                "to the app (Configure → + Add resource → SQL warehouse) so it is "
                "injected via valueFrom."
            )
        return wid

    def _sql_conn(self):
        return sql.connect(
            server_hostname=self._cfg.host,
            http_path=f"/sql/1.0/warehouses/{self._warehouse_id()}",
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

    def offers(self) -> list[dict]:
        """The full offer catalog — the candidate set we rank in one shot."""
        q = (f"SELECT offer_id, product_category, offer_text, base_reward, tier_requirement "
             f"FROM `{CATALOG}`.{SCHEMA}.offers")
        with self._sql_conn() as conn, conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def latency_results(self) -> list[dict]:
        q = (f"SELECT stage, p50, p95, p99 "
             f"FROM `{CATALOG}`.{SCHEMA}.part1_latency_results")
        with self._sql_conn() as conn, conn.cursor() as cur:
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    # --- rank-all recommend -----------------------------------------------
    def recommend(self, customer: dict, offers: list[dict], k: int = 10) -> Recommendation:
        """Score the entire offer catalog for this customer, return the top-k.

        Mirrors the feature-serving benchmark: request carries only customer_id + offer fields; the
        endpoint looks up the customer's features online by key. No retrieval stage.
        """
        timing = Timing()

        recs = [{
            "customer_id": customer["customer_id"],
            "offer_id": o["offer_id"],
            "product_category": o["product_category"],
            "base_reward": float(o["base_reward"]),
            "tier_requirement": int(o["tier_requirement"]),
        } for o in offers]

        # Route-optimized endpoint → data-plane client (data-plane URL + downscoped OAuth token).
        t0 = time.perf_counter()
        preds = self._w.serving_endpoints_data_plane.query(
            name=RANKER_ENDPOINT, dataframe_records=recs
        ).predictions
        timing.rank_ms = (time.perf_counter() - t0) * 1000

        scored = [{
            "offer_id": o["offer_id"],
            "product_category": o["product_category"],
            "offer_text": o["offer_text"],
            "score": float(p),
        } for o, p in zip(offers, preds)]
        scored.sort(key=lambda o: o["score"], reverse=True)
        return Recommendation(offers=scored[:k], timing=timing)
