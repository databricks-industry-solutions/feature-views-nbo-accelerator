import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: 'server/server.ts',
  unbundle: true,
  // server/vendor/kafka.js is built separately as one self-contained file (tsdown.vendor.config.ts).
  external: (id) => /^[^./]/.test(id) || id.includes('/node_modules/') || /\/vendor\/kafka(\.js)?$/.test(id),
  tsconfig: 'tsconfig.server.json',
  outExtensions: () => ({
    js: '.js',
  }),
});
