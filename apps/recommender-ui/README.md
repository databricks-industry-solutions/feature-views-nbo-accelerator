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

The root bundle owns this App together with its Lakeflow Jobs and dashboard. Build the client and
server, then deploy from the repository root:

```bash
cd apps/recommender-ui
npm ci
npm run build:server
npm run build:client
cd ../..

databricks bundle deploy -t dev -p <profile> \
  --var warehouse_id=<warehouse-id>
```

Route-optimized serving requires an OAuth service principal that can query the endpoints. Store its
client ID and secret in the configured secret scope (default `nbo`) under `sp_client_id` and
`sp_client_secret`.

Start the App after deployment:

```bash
databricks bundle run nbo_recommender_ui -t dev -p <profile> \
  --var warehouse_id=<warehouse-id>
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
