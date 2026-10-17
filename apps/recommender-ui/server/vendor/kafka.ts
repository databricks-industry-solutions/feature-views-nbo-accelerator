// Kafka client + MSK IAM signer, bundled into one file (dist/vendor/kafka.js) at build time because the
// Databricks Apps npm mirror does not serve kafkajs. Built by tsdown.vendor.config.ts.
export { Kafka, logLevel } from 'kafkajs';
export type { Producer } from 'kafkajs';
export { generateAuthTokenFromCredentialsProvider } from 'aws-msk-iam-sasl-signer-js';
