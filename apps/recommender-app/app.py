"""Databricks App — Real-Time Next-Best-Offer demo.

Simulates an in-session banking customer, calls the two-stage recommender
(Vector Search retrieval -> ranking endpoint), and renders the top offers
alongside a live end-to-end latency meter to make the <300ms claim tangible.

TODO:
- Session simulator: emit synthetic in-session events (updates streaming features).
- Recommend button: retrieve candidates -> rank -> show top-N with "why this offer".
- Latency meter: per-stage timings + E2E, color-coded against the 300ms budget.
"""

import os

CATALOG = os.environ.get("CATALOG", "nbo_accelerator")
SCHEMA = os.environ.get("SCHEMA", "main")
RANKER_ENDPOINT = os.environ.get("RANKER_ENDPOINT", "nbo-ranker")
VS_ENDPOINT = os.environ.get("VS_ENDPOINT", "nbo-vs-endpoint")


def main() -> None:
    print("TODO: implement Streamlit/Dash NBO demo app")


if __name__ == "__main__":
    main()
