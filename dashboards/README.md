# NBO AI/BI Dashboard

`nbo_dashboard.json` contains the customer-facing AI/BI Dashboard definition for the accelerator.
It presents:

- serving-latency percentiles;
- offer acceptance by loyalty tier;
- offer mix by product category; and
- benchmark detail for the online feature-read and ranking path.

The root Databricks Asset Bundle deploys the dashboard and binds it to the configured catalog,
schema, and SQL warehouse:

```bash
databricks bundle deploy -t dev -p <profile> \
  --var warehouse_id=<warehouse-id>
```

Run the benchmark notebooks before viewing latency panels so the result tables contain current
measurements from the target workspace.
