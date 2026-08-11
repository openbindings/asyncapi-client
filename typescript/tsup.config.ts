import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    engine: "src/engine.ts",
    analysis: "src/analysis.ts",
    testing: "src/testing.ts"
  },
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true
});
