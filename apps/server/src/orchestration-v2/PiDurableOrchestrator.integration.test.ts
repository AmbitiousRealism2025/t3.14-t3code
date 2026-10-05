/**
 * The orchestrator driving a Pi-Durable instance end to end: message commands
 * go through the event sink, outbox and effect worker to the durable adapter,
 * the real T3.14 worker and its fixture model, and back into projections.
 * Runs only when `T314_WORKER_CLI` points at the worker CLI.
 *
 * The restart case is exploratory S01 evidence: it records what stock T3
 * recovery does to a run whose durable submission outlives the server.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import { makeDurableWorkerManager, makePiDurableAdapterV2 } from "./Adapters/PiDurableAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { materializeFixtureInput, projectionFor } from "./testkit/fixtures/shared.ts";
import {
  runOrchestratorV2Scenario,
  type OrchestratorV2ScenarioStep,
} from "./testkit/OrchestratorScenario.ts";
import { provideDeterministicTestRuntime } from "./testkit/DeterministicRuntime.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

const WORKER_CLI = process.env.T314_WORKER_CLI;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const SCENARIO = "pi_durable_orchestrator";
const INSTANCE_ID = ProviderInstanceId.make("pi-durable");
const MODEL_SELECTION: ModelSelection = { instanceId: INSTANCE_ID, model: "faux/faux-1" };

interface Box {
  readonly workspace: string;
  readonly dataHome: string;
  readonly stateHome: string;
  readonly database: string;
}

const sandbox = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t314-orchestrator-" });
  return {
    workspace: dir,
    dataHome: path.join(dir, "home"),
    stateHome: path.join(dir, "state"),
    database: path.join(dir, "state.sqlite"),
  } satisfies Box;
});

/** A registry holding one durable instance whose worker lives as long as the layer. */
const durableRegistryLayer = (box: Box, tokensPerSecond: number) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const workers = yield* makeDurableWorkerManager({
        launch: {
          command: process.execPath,
          args: [
            WORKER_CLI!,
            "worker",
            "--data-home",
            box.dataHome,
            "--cwd",
            box.workspace,
            "--tools",
            "read",
            "--tokens-per-second",
            String(tokensPerSecond),
          ],
        },
        env: { PATH: process.env.PATH, HOME: process.env.HOME, XDG_STATE_HOME: box.stateHome },
      });
      return ProviderAdapterRegistry.makeSingleLayer(
        makePiDurableAdapterV2({
          instanceId: INSTANCE_ID,
          workers,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          serverConfig: yield* ServerConfig.ServerConfig,
        }),
      );
    }),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        NodeServices.layer,
        ServerConfig.layerTest(box.workspace, { prefix: "t314-orchestrator-config-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
      ),
    ),
  );

const materialize = (
  steps: Parameters<typeof materializeFixtureInput>[0]["fixtureInput"]["steps"],
) =>
  materializeFixtureInput({
    scenario: SCENARIO,
    fixtureInput: { steps },
    driver: ProviderDriverKind.make("pi"),
    modelSelection: MODEL_SELECTION,
  });

const run = (
  box: Box,
  name: string,
  steps: ReadonlyArray<OrchestratorV2ScenarioStep>,
  projectionThreadIds: Effect.Success<ReturnType<typeof materialize>>["projectionThreadIds"],
  options: { readonly tokensPerSecond?: number; readonly recoverOnStartup?: boolean } = {},
) => {
  const scenario = {
    name: `${SCENARIO}:${name}`,
    commands: steps.flatMap((step) => (step.type === "dispatch" ? [step.command] : [])),
    steps,
    projectionThreadIds,
    runtimePolicyOverride: { cwd: box.workspace },
  };
  const databaseLayer = makeSqlitePersistenceLive(box.database).pipe(
    Layer.provide(NodeServices.layer),
  );
  return Effect.scoped(
    Effect.gen(function* () {
      if (options.recoverOnStartup === true) {
        yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
      }
      return yield* runOrchestratorV2Scenario(scenario);
    }).pipe(
      Effect.provide(
        makeOrchestratorV2ReplayLayerWithRegistry(
          scenario,
          durableRegistryLayer(box, options.tokensPerSecond ?? 0),
          {
            databaseLayer,
            ...(options.recoverOnStartup === undefined
              ? {}
              : { recoverOnStartup: options.recoverOnStartup }),
          },
        ),
      ),
    ),
  );
};

/** The durable store's own transcript, read through a fresh worker. */
const durableTranscript = (box: Box, nativeRef: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const conversationId = Number(nativeRef.split(":").at(-1));
      const workers = yield* makeDurableWorkerManager({
        launch: {
          command: process.execPath,
          args: [WORKER_CLI!, "worker", "--data-home", box.dataHome, "--cwd", box.workspace],
        },
        env: { PATH: process.env.PATH, HOME: process.env.HOME, XDG_STATE_HOME: box.stateHome },
      });
      const worker = yield* workers.get;
      const data = yield* worker.request("conversation.entries", { conversationId });
      const entries = (data as { entries?: ReadonlyArray<{ model?: ReadonlyArray<unknown> }> })
        .entries;
      return (entries ?? []).flatMap((entry) => {
        const message = entry.model?.[0] as
          | { role?: string; content?: unknown; stopReason?: string }
          | undefined;
        if (message?.role !== "user" && message?.role !== "assistant") return [];
        const content = message.content;
        const text =
          typeof content === "string"
            ? content
            : Array.isArray(content)
              ? content.map((block) => (block?.type === "text" ? block.text : "")).join("")
              : "";
        return [
          {
            role: message.role,
            text: text.length > 60 ? `${text.slice(0, 60)}…` : text,
            ...(message.stopReason === undefined ? {} : { stopReason: message.stopReason }),
          },
        ];
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

const assistantTexts = (projection: OrchestrationV2ThreadProjection) =>
  projection.turnItems.flatMap((item) => (item.type === "assistant_message" ? [item.text] : []));

describe.skipIf(WORKER_CLI === undefined)(
  "Pi-Durable through the orchestrator (real worker)",
  () => {
    it.effect("runs two turns on one durable conversation", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const input = yield* materialize([
          { type: "message", text: "first durable turn" },
          { type: "message", text: "second durable turn" },
        ]);
        const result = yield* run(box, "two-turns", input.steps, input.projectionThreadIds);
        const projection = projectionFor(result, SCENARIO);
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["completed", "completed"],
        );
        assert.deepEqual(assistantTexts(projection), [
          "echo: first durable turn",
          "echo: second durable turn",
        ]);
        assert.lengthOf(projection.providerThreads, 1);
        assert.match(
          projection.providerThreads[0]?.nativeThreadRef?.nativeId ?? "",
          /^pi-durable:[0-9a-f-]+:\d+$/,
        );
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );

    it.effect("Stop interrupts a streaming durable run", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const input = yield* materialize([
          { type: "message", text: "slow: stop me" },
          { type: "interrupt", targetRunIndex: 1 },
        ]);
        const result = yield* run(box, "interrupt", input.steps, input.projectionThreadIds, {
          tokensPerSecond: 20,
        });
        const projection = projectionFor(result, SCENARIO);
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );

    it.effect("exploratory: a server restart mid-turn under stock recovery", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const input = yield* materialize([
          { type: "message", text: "slow: survive the restart" },
          { type: "message", text: "after the restart" },
        ]);
        const messageAt = input.steps.flatMap((step, index) =>
          step.type === "dispatch" && step.command.type === "message.dispatch" ? [index] : [],
        );
        const [firstMessage, secondMessage] = messageAt;
        const firstDispatch = firstMessage === undefined ? undefined : input.steps[firstMessage];
        assert.isTrue(firstDispatch?.type === "dispatch" && secondMessage !== undefined);
        if (
          firstDispatch?.type !== "dispatch" ||
          firstMessage === undefined ||
          secondMessage === undefined
        ) {
          return;
        }
        const threadId = input.projectionThreadIds[0]!;
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });

        // Phase 1: the server stops while the first answer is still being generated.
        const before = yield* run(
          box,
          "before-restart",
          [
            ...input.steps.slice(0, firstMessage),
            { ...firstDispatch, await: false, key: "run:1" },
            { type: "await_run_steerable", threadId, runId },
          ],
          input.projectionThreadIds,
          { tokensPerSecond: 20 },
        );
        assert.equal(projectionFor(before, SCENARIO).runs[0]?.status, "running");

        // Phase 2: stock startup recovery runs against the same T3 and durable stores.
        const recovered = yield* run(box, "recovered", [], input.projectionThreadIds, {
          recoverOnStartup: true,
        });
        const afterRecovery = projectionFor(recovered, SCENARIO);

        // Phase 3: the user sends the next message.
        const after = yield* run(
          box,
          "after-restart",
          input.steps.slice(secondMessage),
          input.projectionThreadIds,
        );
        const projection = projectionFor(after, SCENARIO);

        // Recorded, not endorsed: what stock recovery does to the T3 run while
        // the durable submission survives in the worker's store.
        const observation = {
          runStatusesAfterRecovery: afterRecovery.runs.map((entry) => entry.status),
          runStatusesAfterNextMessage: projection.runs.map((entry) => entry.status),
          turnItemsAfterNextMessage: projection.turnItems.map((item) => ({
            runId: item.runId,
            type: item.type,
            status: item.status,
            ...(item.type === "assistant_message" || item.type === "user_message"
              ? { text: item.text }
              : {}),
          })),
          providerThreads: projection.providerThreads.map(
            (thread) => thread.nativeThreadRef?.nativeId ?? null,
          ),
          durableTranscript: yield* durableTranscript(
            box,
            projection.providerThreads[0]?.nativeThreadRef?.nativeId ?? "",
          ),
        };
        const evidenceDir = process.env.T314_EVIDENCE_DIR;
        if (evidenceDir !== undefined) {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          yield* fs.writeFileString(
            path.join(evidenceDir, "s01-orchestrator-restart.json"),
            `${encodeJson(observation)}\n`,
          );
        }
        assert.equal(afterRecovery.runs[0]?.status, "cancelled");
        assert.equal(projection.runs[1]?.status, "completed");
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );
  },
);
