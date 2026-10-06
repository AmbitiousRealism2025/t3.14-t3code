// @effect-diagnostics nodeBuiltinImport:off - A local OpenAI-compatible endpoint stands in for a real provider.
/**
 * The orchestrator driving a Pi-Durable instance end to end: message commands
 * go through the event sink, outbox and effect worker to the durable adapter,
 * the real T3.14 worker and its fixture model, and back into projections.
 * Runs only when `T314_WORKER_CLI` points at the worker CLI.
 *
 * The restart cases cover W02: startup recovery leaves a run bound to a
 * durable conversation running, the restarted worker keeps that
 * conversation's work, and a reattach adopts it, so the run finishes under
 * its original T3 run in both stores.
 */
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";
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
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import { makeDurableWorkerManager, makePiDurableAdapterV2 } from "./Adapters/PiDurableAdapterV2.ts";
import * as DurableReattach from "./DurableReattach.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as Orchestrator from "./Orchestrator.ts";
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

type ReattachPlan = DurableReattach.DurableReattachPlan["Service"];

/** The worker on Pi's models (`--models pi`), with Pi's agent directory in `env`. */
interface PiWorker {
  readonly env: Readonly<Record<string, string>>;
}

/** A registry holding one durable instance whose worker lives as long as the layer. */
const durableRegistryLayer = (
  box: Box,
  tokensPerSecond: number,
  plan?: ReattachPlan,
  pi?: PiWorker,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const workers = yield* makeDurableWorkerManager({
        ...(plan === undefined ? {} : { keepConversations: plan.conversationsFor(INSTANCE_ID) }),
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
            ...(pi === undefined ? [] : ["--models", "pi"]),
          ],
        },
        env: {
          PATH: process.env.PATH,
          HOME: process.env.HOME,
          XDG_STATE_HOME: box.stateHome,
          ...pi?.env,
        },
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
  modelSelection: ModelSelection = MODEL_SELECTION,
) =>
  materializeFixtureInput({
    scenario: SCENARIO,
    fixtureInput: { steps },
    driver: ProviderDriverKind.make("pi"),
    modelSelection,
  });

const run = (
  box: Box,
  name: string,
  steps: ReadonlyArray<OrchestratorV2ScenarioStep>,
  projectionThreadIds: Effect.Success<ReturnType<typeof materialize>>["projectionThreadIds"],
  options: {
    readonly tokensPerSecond?: number;
    /** Startup recovery with a reattach plan, as server startup runs it after a restart. */
    readonly recoverOnStartup?: boolean;
    readonly pi?: PiWorker;
    /** Runs after the steps, before this server stops. */
    readonly until?: Effect.Effect<
      void,
      Orchestrator.OrchestratorV2Error,
      Orchestrator.OrchestratorV2
    >;
  } = {},
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
      const plan = options.recoverOnStartup === true ? yield* DurableReattach.make : undefined;
      return yield* Effect.gen(function* () {
        if (options.recoverOnStartup === true) {
          yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
        }
        const result = yield* runOrchestratorV2Scenario(scenario);
        if (options.until !== undefined) yield* options.until;
        return result;
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            scenario,
            durableRegistryLayer(box, options.tokensPerSecond ?? 0, plan, options.pi),
            {
              databaseLayer,
              ...(options.recoverOnStartup === undefined
                ? {}
                : { recoverOnStartup: options.recoverOnStartup }),
            },
          ).pipe(
            Layer.provide(
              plan === undefined
                ? Layer.empty
                : Layer.succeed(DurableReattach.DurableReattachPlan, plan),
            ),
          ),
        ),
      );
    }),
  );
};

/** Waits until a session reattached after the last restart is running the thread's turn. */
const awaitReattachedTurn = (threadId: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    let stopped = false;
    yield* orchestrator.streamStoredEvents.pipe(
      Stream.map((stored) => stored.event),
      Stream.filter(
        (event) => event.threadId === threadId && event.type === "provider-session.updated",
      ),
      Stream.takeUntil((event) => {
        if (event.type !== "provider-session.updated") return false;
        if (event.payload.status === "stopped") stopped = true;
        return stopped && event.payload.status === "running";
      }),
      Stream.runDrain,
    );
  });

/** Durable data held by another store: the box lost its store, or it was replaced. */
const durableConversationExists = (
  dataHome: string,
  workspace: string,
  stateHome: string,
  conversationId: number,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const workers = yield* makeDurableWorkerManager({
        launch: {
          command: process.execPath,
          args: [WORKER_CLI!, "worker", "--data-home", dataHome, "--cwd", workspace],
        },
        env: { PATH: process.env.PATH, HOME: process.env.HOME, XDG_STATE_HOME: stateHome },
      });
      const worker = yield* workers.get;
      return yield* worker.request("conversation.entries", { conversationId }).pipe(
        Effect.as(true),
        Effect.catchIf(
          (error) => error.errorName === "ConversationNotFoundError",
          () => Effect.succeed(false),
        ),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

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

/** The scenario steps around the first and second user message. */
const messageSteps = (steps: ReadonlyArray<OrchestratorV2ScenarioStep>) => {
  const messageAt = steps.flatMap((step, index) =>
    step.type === "dispatch" && step.command.type === "message.dispatch" ? [index] : [],
  );
  const firstMessage = messageAt[0]!;
  const firstDispatch = steps[firstMessage];
  if (firstDispatch?.type !== "dispatch") throw new Error("the scenario sends no message");
  return { firstDispatch, firstMessage, secondMessage: messageAt[1] ?? steps.length };
};

/**
 * A local OpenAI-compatible endpoint that answers every chat completion with
 * one streamed reply. Pi's real `openai-completions` client talks to it.
 */
const localOpenAiEndpoint = (reply: string) =>
  Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<{
          readonly server: NodeHttp.Server;
          readonly port: number;
          readonly models: Array<string | undefined>;
        }>((resolve) => {
          const models: Array<string | undefined> = [];
          const chunk = (choices: unknown[], extra: object = {}) =>
            `data: ${JSON.stringify({ id: "c1", object: "chat.completion.chunk", created: 0, choices, ...extra })}\n\n`;
          const server = NodeHttp.createServer((request, response) => {
            let raw = "";
            request.on("data", (part) => (raw += String(part)));
            request.on("end", () => {
              models.push((JSON.parse(raw || "{}") as { model?: string }).model);
              response.writeHead(200, { "content-type": "text/event-stream" });
              response.end(
                [
                  chunk([
                    { index: 0, delta: { role: "assistant", content: "" }, finish_reason: null },
                  ]),
                  ...reply.split(" ").map((word, index) =>
                    chunk([
                      {
                        index: 0,
                        delta: { content: index === 0 ? word : ` ${word}` },
                        finish_reason: null,
                      },
                    ]),
                  ),
                  chunk([{ index: 0, delta: {}, finish_reason: "stop" }]),
                  chunk([], {
                    usage: { prompt_tokens: 9, completion_tokens: 4, total_tokens: 13 },
                  }),
                  "data: [DONE]\n\n",
                ].join(""),
              );
            });
          });
          server.listen(0, "127.0.0.1", () =>
            resolve({ server, port: (server.address() as NodeNet.AddressInfo).port, models }),
          );
        }),
    ),
    ({ server }) => Effect.promise(() => new Promise<void>((done) => server.close(() => done()))),
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

    it.effect(
      "a server restart mid-turn reattaches the run, which finishes in both stores (W02)",
      () =>
        Effect.gen(function* () {
          const box = yield* sandbox;
          const idAllocator = yield* IdAllocator.IdAllocatorV2;
          const input = yield* materialize([
            { type: "message", text: "slow: survive the restart" },
            { type: "message", text: "after the restart" },
          ]);
          const { firstDispatch, firstMessage, secondMessage } = messageSteps(input.steps);
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

          // Phase 2: startup recovery keeps the run; the reattach sees it finish.
          yield* TestClock.adjust("1 second");
          const recovered = yield* run(
            box,
            "recovered",
            [{ type: "await_run_status", threadId, runId, status: "completed" }],
            input.projectionThreadIds,
            { recoverOnStartup: true, tokensPerSecond: 200 },
          );
          const afterRecovery = projectionFor(recovered, SCENARIO);

          // Phase 3: the user sends the next message.
          const after = yield* run(
            box,
            "after-restart",
            input.steps.slice(secondMessage),
            input.projectionThreadIds,
          );
          const projection = projectionFor(after, SCENARIO);

          // What a restart now leaves in T3 and in the durable store.
          const observation = {
            runStatusesAfterRecovery: afterRecovery.runs.map((entry) => entry.status),
            runStatusesAfterNextMessage: projection.runs.map((entry) => entry.status),
            turnItemsAfterNextMessage: projection.turnItems.map((item) => ({
              runId: item.runId,
              type: item.type,
              status: item.status,
              ...(item.type === "assistant_message" || item.type === "user_message"
                ? { text: item.text.length > 60 ? `${item.text.slice(0, 60)}…` : item.text }
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
          const answer = "survive the restart ".repeat(40).trim();
          assert.deepEqual(observation.runStatusesAfterRecovery, ["completed"]);
          assert.deepEqual(observation.runStatusesAfterNextMessage, ["completed", "completed"]);
          // One answer per run: the reattach updated the item projected before the restart.
          assert.deepEqual(assistantTexts(projection), [answer, "echo: after the restart"]);
          assert.lengthOf(projection.providerThreads, 1);
          assert.deepEqual(
            observation.durableTranscript.flatMap((entry) =>
              entry.role === "assistant" && entry.stopReason === "stop" ? [entry.text] : [],
            ),
            [`${answer.slice(0, 60)}…`, "echo: after the restart"],
          );
        }).pipe(
          Effect.scoped,
          provideDeterministicTestRuntime,
          Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
        ),
    );

    it.effect("a steer sent just before a restart is answered once, in the same run", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const input = yield* materialize([
          { type: "message", text: "slow: keep going" },
          { type: "steer", text: "and then this", targetRunIndex: 1 },
        ]);
        const threadId = input.projectionThreadIds[0]!;
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        // Everything up to the steer's dispatch; its delivery may or may not
        // run before the server stops.
        const steerAt = input.steps.findIndex(
          (step) =>
            step.type === "dispatch" &&
            step.command.type === "message.dispatch" &&
            step.command.dispatchMode?.type === "steer_active",
        );
        assert.isAbove(steerAt, 0);
        yield* run(
          box,
          "before-restart",
          input.steps.slice(0, steerAt + 1),
          input.projectionThreadIds,
          {
            tokensPerSecond: 20,
          },
        );
        yield* TestClock.adjust("1 second");
        const recovered = yield* run(
          box,
          "recovered",
          [{ type: "await_run_status", threadId, runId, status: "completed" }],
          input.projectionThreadIds,
          { recoverOnStartup: true, tokensPerSecond: 200 },
        );
        const projection = projectionFor(recovered, SCENARIO);
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["completed"],
        );
        assert.include(assistantTexts(projection), "echo: and then this");
        const transcript = yield* durableTranscript(
          box,
          projection.providerThreads[0]?.nativeThreadRef?.nativeId ?? "",
        );
        assert.deepEqual(
          transcript.flatMap((entry) => (entry.role === "user" ? [entry.text] : [])),
          ["slow: keep going", "and then this"],
          "the steer reached the durable store exactly once",
        );
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );

    it.effect("a second restart while the reattached run streams reattaches it again", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const input = yield* materialize([{ type: "message", text: "slow: survive two restarts" }]);
        const { firstDispatch, firstMessage } = messageSteps(input.steps);
        const threadId = input.projectionThreadIds[0]!;
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });

        yield* run(
          box,
          "before-restarts",
          [
            ...input.steps.slice(0, firstMessage),
            { ...firstDispatch, await: false, key: "run:1" },
            { type: "await_run_steerable", threadId, runId },
          ],
          input.projectionThreadIds,
          { tokensPerSecond: 20 },
        );
        // The first restart stops again once the reattached turn is streaming.
        yield* TestClock.adjust("1 second");
        yield* run(box, "first-restart", [], input.projectionThreadIds, {
          recoverOnStartup: true,
          tokensPerSecond: 20,
          until: awaitReattachedTurn(threadId),
        });
        yield* TestClock.adjust("1 second");
        const recovered = yield* run(
          box,
          "second-restart",
          [{ type: "await_run_status", threadId, runId, status: "completed" }],
          input.projectionThreadIds,
          { recoverOnStartup: true, tokensPerSecond: 200 },
        );
        const projection = projectionFor(recovered, SCENARIO);
        const answer = "survive two restarts ".repeat(40).trim();
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["completed"],
        );
        assert.deepEqual(assistantTexts(projection), [answer]);
        const transcript = yield* durableTranscript(
          box,
          projection.providerThreads[0]?.nativeThreadRef?.nativeId ?? "",
        );
        assert.deepEqual(
          transcript.flatMap((entry) =>
            entry.role === "assistant" && entry.stopReason === "stop" ? [entry.text] : [],
          ),
          [`${answer.slice(0, 60)}…`],
        );
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );

    it.effect("a run whose conversation the restarted store lacks fails without a new one", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const input = yield* materialize([{ type: "message", text: "slow: lost with its store" }]);
        const { firstDispatch, firstMessage } = messageSteps(input.steps);
        const threadId = input.projectionThreadIds[0]!;
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });

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
        const nativeRef = projectionFor(before, SCENARIO).providerThreads[0]?.nativeThreadRef;
        // The server comes back on a different durable store.
        const replaced = { ...box, dataHome: `${box.dataHome}-replaced` };
        yield* TestClock.adjust("1 second");
        const recovered = yield* run(
          replaced,
          "replaced-store",
          [{ type: "await_run_status", threadId, runId, status: "failed" }],
          input.projectionThreadIds,
          { recoverOnStartup: true },
        );
        const projection = projectionFor(recovered, SCENARIO);
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["failed"],
        );
        assert.isTrue(
          projection.turnItems.some(
            (item) => item.type === "error" && item.title === "Could not reattach the durable run",
          ),
        );
        // The thread still names the old conversation, and the new store opened none.
        assert.deepEqual(projection.providerThreads[0]?.nativeThreadRef, nativeRef);
        const conversationId = Number(nativeRef?.nativeId?.split(":").at(-1));
        assert.isFalse(
          yield* durableConversationExists(
            replaced.dataHome,
            replaced.workspace,
            replaced.stateHome,
            conversationId,
          ),
        );
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );

    it.effect("Pi's model client streams a provider answer through the orchestrator", () =>
      Effect.gen(function* () {
        const box = yield* sandbox;
        const endpoint = yield* localOpenAiEndpoint("hello from the fake model");
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        // An isolated Pi agent directory whose only provider is the local endpoint.
        const agentDir = path.join(path.dirname(box.dataHome), "pi-agent");
        const home = path.join(path.dirname(box.dataHome), "user-home");
        yield* fs.makeDirectory(agentDir, { recursive: true });
        yield* fs.makeDirectory(home, { recursive: true });
        yield* fs.writeFileString(
          path.join(agentDir, "models.json"),
          encodeJson({
            providers: {
              fakeai: {
                baseUrl: `http://127.0.0.1:${endpoint.port}/v1`,
                api: "openai-completions",
                apiKey: "test-key",
                models: [{ id: "fake-1", name: "Fake One", contextWindow: 32000 }],
              },
            },
          }),
        );
        const input = yield* materialize([{ type: "message", text: "say hello" }], {
          instanceId: INSTANCE_ID,
          model: "fakeai/fake-1",
        });
        const result = yield* run(box, "pi-models", input.steps, input.projectionThreadIds, {
          pi: { env: { HOME: home, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1" } },
        });
        const projection = projectionFor(result, SCENARIO);
        assert.deepEqual(
          projection.runs.map((entry) => entry.status),
          ["completed"],
        );
        assert.deepEqual(assistantTexts(projection), ["hello from the fake model"]);
        assert.deepEqual(endpoint.models, ["fake-1"], "one provider call, to the selected model");
      }).pipe(
        Effect.scoped,
        provideDeterministicTestRuntime,
        Effect.provide(Layer.merge(IdAllocator.layer, NodeServices.layer)),
      ),
    );
  },
);
