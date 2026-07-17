"""Load-test harness for the <300ms end-to-end serving path (mirrors notebook 06).

Fires concurrent requests through the two-stage recommender, records per-stage
and end-to-end timings, and reports p50/p95/p99. Warm the endpoints before
measuring (scale-to-zero cold starts skew p99).
"""

# TODO: async request generator; per-stage timing capture; percentile aggregation;
#       write results to Delta for the latency dashboard.
