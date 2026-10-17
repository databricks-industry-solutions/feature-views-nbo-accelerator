import { defineConfig } from 'tsdown';

// The Databricks Apps npm mirror does not serve kafkajs, so the Kafka client and the MSK IAM signer (with
// its AWS SDK dependencies) are bundled into dist/vendor/kafka.js; they are devDependencies only.
export default defineConfig({
  entry: { kafka: 'server/vendor/kafka.ts' },
  outDir: 'dist/vendor',
  clean: false,
  platform: 'node',
  noExternal: [/.*/],
  tsconfig: 'tsconfig.server.json',
  outExtensions: () => ({
    js: '.js',
  }),
});
