"""Real-Time Next-Best-Offer — Databricks App (Streamlit).

Simulates a retail-banking customer active in-session, runs the two-stage
recommender (Vector Search retrieval → Model Serving ranking), and renders the
ranked offers alongside a live end-to-end latency meter that makes the sub-300ms
serving claim tangible.
"""

import streamlit as st

from backend import Backend

st.set_page_config(page_title="Next-Best-Offer · Feature Views", layout="wide")

BUDGET_MS = 300  # the headline serving budget

# In-session intent presets — each maps to a natural-language retrieval context.
CONTEXTS = {
    "Comparing credit cards": "customer comparing credit cards with cashback and travel rewards",
    "Viewing savings rates": "customer viewing savings account rates and high yield options",
    "Using loan calculator": "customer using personal loan calculator for debt consolidation",
    "Browsing mortgages": "customer browsing mortgage refinance and home equity options",
    "Exploring investments": "customer exploring retirement investment and IRA products",
}


@st.cache_resource
def get_backend() -> Backend:
    return Backend()


@st.cache_data(ttl=300)
def load_customers() -> list[dict]:
    return get_backend().sample_customers(25)


@st.cache_data(ttl=300)
def load_latency() -> list[dict]:
    try:
        return get_backend().latency_results()
    except Exception:
        return []


def meter(label: str, ms: float, budget: float | None = None) -> None:
    """Render one latency stat, colored against its budget."""
    if budget is None:
        color = "normal"
    else:
        color = "normal" if ms < budget else "inverse"
    st.metric(label, f"{ms:.0f} ms",
              delta=(f"budget {budget:.0f} ms" if budget else None),
              delta_color=color)


st.title("🏦 Real-Time Next-Best-Offer")
st.caption("Two-stage recommender on Databricks Feature Views — "
           "in-session intent → Vector Search retrieval → ranking, in **under 300ms**.")

backend = get_backend()

# --- Controls -------------------------------------------------------------
left, right = st.columns([1, 2])

with left:
    st.subheader("Customer session")
    try:
        customers = load_customers()
    except Exception as e:
        st.error(f"Could not load customers: {e}")
        st.stop()

    labels = {f"{c['customer_id']} · {c['loyalty_tier']}/{c['risk_band']}": c
              for c in customers}
    picked = st.selectbox("Customer", list(labels.keys()))
    customer = labels[picked]

    c1, c2 = st.columns(2)
    c1.metric("Loyalty tier", customer["loyalty_tier"])
    c2.metric("Risk band", customer["risk_band"])

    intent = st.radio("In-session intent", list(CONTEXTS.keys()))
    go = st.button("🎯 Recommend offers", type="primary", use_container_width=True)

# --- Recommend + latency meter -------------------------------------------
with right:
    st.subheader("Recommended offers")
    if go:
        try:
            rec = backend.recommend(customer, CONTEXTS[intent], k=10)
        except Exception as e:
            st.error(f"Recommendation failed: {e}")
            st.stop()

        m1, m2, m3 = st.columns(3)
        with m1:
            meter("Retrieval", rec.timing.retrieval_ms)
        with m2:
            meter("Ranking", rec.timing.ranking_ms)
        with m3:
            meter("End-to-end", rec.timing.e2e_ms, budget=BUDGET_MS)

        if rec.timing.e2e_ms < BUDGET_MS:
            st.success(f"✅ Served in {rec.timing.e2e_ms:.0f} ms — under the {BUDGET_MS} ms budget.")
        else:
            st.warning(f"⚠️ Served in {rec.timing.e2e_ms:.0f} ms — over the {BUDGET_MS} ms budget "
                       "(likely a cold slot; retry).")

        # Raw acceptance probabilities often saturate near 1.0 for strong profiles, so a
        # 0–1 bar looks flat. Show explicit rank + a *relative* bar (min-max normalized
        # within this result set) so the ordering is legible, with the raw score alongside.
        scores = [o["score"] for o in rec.offers]
        lo, hi = min(scores), max(scores)
        span = (hi - lo) or 1.0
        rows = [{
            "Rank": i + 1,
            "Offer": o["offer_id"],
            "Category": o["product_category"],
            "Relative": (o["score"] - lo) / span,
            "Score": round(o["score"], 4),
            "Description": o["offer_text"],
        } for i, o in enumerate(rec.offers)]

        st.dataframe(
            rows, use_container_width=True, hide_index=True,
            column_config={
                "Relative": st.column_config.ProgressColumn(
                    "Relative rank", min_value=0.0, max_value=1.0, format="%.2f",
                    help="Min-max normalized within these candidates — shows ordering, not absolute probability."),
                "Score": st.column_config.NumberColumn(
                    "Raw P(accept)", format="%.4f",
                    help="Model's raw acceptance probability; saturates near 1.0 for high-value profiles."),
            },
        )
    else:
        st.info("Pick a customer and an in-session intent, then click **Recommend offers**.")

# --- Benchmark reference ---------------------------------------------------
st.divider()
st.subheader("📊 Benchmarked serving latency (notebook 06)")
lat = load_latency()
if lat:
    st.caption("Percentiles from the load test (N=100, warmed). Read live from "
               "`latency_results`.")
    st.dataframe(
        [{"Stage": r["stage"], "p50 (ms)": r["p50"], "p95 (ms)": r["p95"], "p99 (ms)": r["p99"]}
         for r in sorted(lat, key=lambda r: r["stage"])],
        use_container_width=True, hide_index=True,
    )
else:
    st.caption("Run notebook 06 to populate `latency_results`.")
