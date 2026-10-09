# Next-Best-Offer Databricks App

The customer-facing application for the Real-Time Next-Best-Offer accelerator. It combines a
banking storefront, personalized product recommendations, a live scale dashboard, and an
interactive architecture walkthrough.

## Experience

- **OfferMatch:** customers select goals and preferences, then receive ranked offers.
- **Product journeys:** cards, savings, loans, mortgages, and investing pages update recommendations
  from request-time inputs.
- **Live signals:** Model Serving ranks offers while Feature Serving returns governed customer and
  streaming features.
- **Operational dashboard:** explicit load testing, Kafka throughput, and event-to-online freshness.
- **Architecture walkthrough:** shows how Kafka, Feature Views, Lakebase, training, and serving work
  together.

## Technology

- React, TypeScript, Vite, and AppKit
- Express server routes for ranking, profile lookup, traffic control, and metrics
- Route-optimized Model Serving and Feature Serving endpoints
- Lakeflow Jobs for the synthetic traffic producer
- SQL warehouse access for governed accelerator data

## Local validation

```bash
npm ci
npm run typecheck
npx vitest run shared/recommendation.test.ts
npm run build:server
npm run build:client
```

## Deploy

The app is a separate Databricks Asset Bundle. Deploy the accelerator first so the SQL warehouse,
serving endpoints, Feature Serving endpoint, traffic job, and secret scope already exist.

```bash
databricks apps deploy -t dev -p <profile> --auto-approve \
  --var app_name=<app-name> \
  --var sql_warehouse_id=<warehouse-id> \
  --var catalog=<catalog> \
  --var schema=<schema> \
  --var ranker_endpoint=<realtime-ranker-endpoint> \
  --var feature_endpoint=<feature-serving-endpoint> \
  --var traffic_job_id=<traffic-job-id>
```

Route-optimized serving requires an OAuth service principal that can query the endpoints. Store its
client ID and secret in the configured secret scope (default `nbo`) under `sp_client_id` and
`sp_client_secret`.

After deployment:

```bash
databricks apps get <app-name> -p <profile>
```

## Project structure

| Path | Purpose |
|---|---|
| `client/src/views/BankView.tsx` | Customer experience and product journeys |
| `client/src/views/DashboardView.tsx` | Live serving and freshness dashboard |
| `client/src/views/HowView.tsx` | Architecture walkthrough |
| `server/server.ts` | App APIs and serving integration |
| `server/realtime.ts` | Traffic, throughput, freshness, and latency metrics |
| `shared/recommendation.ts` | Recommendation suitability contract |
