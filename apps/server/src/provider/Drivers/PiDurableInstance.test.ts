import { assert, describe, it } from "@effect/vitest";

import { EMPTY_PI_MODEL_CAPABILITIES } from "../Layers/piThinkingCapabilities.ts";
import { durableProviderProbe } from "./PiDurableInstance.ts";

const models = [
  { provider: "faux", modelId: "faux-1", name: "Fixture", contextWindow: 1000, reasoning: false },
  { provider: "faux", modelId: "faux-2", name: "Fixture 2", contextWindow: 1000, reasoning: true },
];
const thinkingLevels = (capabilities: unknown) =>
  (
    capabilities as {
      readonly optionDescriptors?: ReadonlyArray<{
        readonly id: string;
        readonly options?: ReadonlyArray<{ readonly id: string }>;
      }>;
    }
  ).optionDescriptors
    ?.find((descriptor) => descriptor.id === "thinking")
    ?.options?.map((option) => option.id) ?? [];
const enabled = { storeId: "store", ownerEpoch: 1, scheduling: "enabled", reasons: [] };

describe("durableProviderProbe", () => {
  it("leads the catalog with Pi's default, named after the model it resolves to", () => {
    const { probe, models: catalog } = durableProviderProbe({
      status: enabled,
      models,
      defaultModel: { provider: "faux", modelId: "faux-2" },
    });
    assert.strictEqual(probe.status, "ready");
    assert.deepStrictEqual(
      catalog.map((model) => [model.slug, model.name]),
      [
        ["default", "Pi default (Fixture 2)"],
        ["faux/faux-1", "Fixture"],
        ["faux/faux-2", "Fixture 2"],
      ],
    );
  });

  it("offers thinking levels for reasoning models, including through the default entry", () => {
    const { models: catalog } = durableProviderProbe({
      status: enabled,
      models: [
        ...models,
        {
          provider: "faux",
          modelId: "faux-3",
          name: "Fixture 3",
          contextWindow: 1000,
          reasoning: true,
          thinkingLevelMap: { xhigh: "xhigh", minimal: null },
        },
      ],
      defaultModel: { provider: "faux", modelId: "faux-2" },
    });
    const bySlug = new Map(catalog.map((model) => [model.slug, model.capabilities]));
    assert.deepStrictEqual(bySlug.get("faux/faux-1"), EMPTY_PI_MODEL_CAPABILITIES);
    assert.deepStrictEqual(thinkingLevels(bySlug.get("faux/faux-2")), [
      "off",
      "minimal",
      "low",
      "medium",
      "high",
    ]);
    assert.deepStrictEqual(
      thinkingLevels(bySlug.get("default")),
      thinkingLevels(bySlug.get("faux/faux-2")),
    );
    assert.deepStrictEqual(thinkingLevels(bySlug.get("faux/faux-3")), [
      "off",
      "low",
      "medium",
      "high",
      "xhigh",
    ]);
  });

  it("omits the default entry when the worker has no model", () => {
    const { probe, models: catalog } = durableProviderProbe({
      status: enabled,
      models: [],
      defaultModel: null,
    });
    assert.deepStrictEqual(catalog, []);
    assert.strictEqual(probe.auth.status, "unauthenticated");
  });

  it("explains how to leave inspection without a running owner", () => {
    const { probe } = durableProviderProbe({
      status: { ...enabled, scheduling: "inhibited", reasons: ["operator-requested"] },
      models,
      defaultModel: { provider: "faux", modelId: "faux-1" },
    });
    assert.strictEqual(probe.status, "warning");
    assert.include(probe.message ?? "", "operator-requested");
    assert.include(probe.message ?? "", "Stop T3.14");
  });
});
