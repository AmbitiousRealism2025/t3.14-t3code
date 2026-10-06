import { assert, it, vi } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as DurableReattach from "./DurableReattach.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as ServerSettings from "../serverSettings.ts";

const STORE_ID = "00000000-0000-4000-8000-00000000000a";
const threadId = ThreadId.make("thread_durable_reattach");
const durableInstance = ProviderInstanceId.make("pi-durable");
const codexInstance = ProviderInstanceId.make("codex");
const durableRun = RunId.make("run_durable");
const codexRun = RunId.make("run_codex");
const durableThread = ProviderThreadId.make("provider_thread_durable");
const codexThread = ProviderThreadId.make("provider_thread_codex");
const durableAttempt = RunAttemptId.make("attempt_durable");
const codexAttempt = RunAttemptId.make("attempt_codex");

/** A thread with a run on a durable conversation and, optionally, a stock provider run. */
const recoveryProjection = (options: { readonly withCodexRun: boolean }) =>
  ({
    thread: { id: threadId, archivedAt: null, deletedAt: null },
    runtimeRequests: [],
    providerSessions: [],
    providerThreads: [
      {
        id: durableThread,
        driver: ProviderDriverKind.make("pi"),
        providerInstanceId: durableInstance,
        nativeThreadRef: { driver: "pi", nativeId: `pi-durable:${STORE_ID}:7`, strength: "strong" },
        status: "active",
        pendingBackgroundTasks: [],
      },
      ...(options.withCodexRun
        ? [
            {
              id: codexThread,
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId: codexInstance,
              nativeThreadRef: { driver: "codex", nativeId: "thr_codex", strength: "strong" },
              status: "active",
              pendingBackgroundTasks: [],
            },
          ]
        : []),
    ],
    providerTurns: [
      {
        id: ProviderTurnId.make("provider_turn_durable"),
        runAttemptId: durableAttempt,
        status: "running",
      },
      ...(options.withCodexRun
        ? [
            {
              id: ProviderTurnId.make("provider_turn_codex"),
              runAttemptId: codexAttempt,
              status: "running",
            },
          ]
        : []),
    ],
    runs: [
      {
        id: durableRun,
        status: "running",
        activeAttemptId: durableAttempt,
        providerInstanceId: durableInstance,
        providerThreadId: durableThread,
      },
      ...(options.withCodexRun
        ? [
            {
              id: codexRun,
              status: "running",
              activeAttemptId: codexAttempt,
              providerInstanceId: codexInstance,
              providerThreadId: codexThread,
            },
          ]
        : []),
    ],
    attempts: [
      { id: durableAttempt, runId: durableRun, status: "running" },
      ...(options.withCodexRun ? [{ id: codexAttempt, runId: codexRun, status: "running" }] : []),
    ],
    nodes: [],
    subagents: [],
    messages: [],
    turnItems: [
      {
        id: TurnItemId.make("turn_item_durable_tool"),
        runId: durableRun,
        nodeId: null,
        providerThreadId: durableThread,
        type: "dynamic_tool",
        status: "running",
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

type CommitInput = Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0];
type WriteInput = Parameters<EventSink.EventSinkV2["Service"]["writeWithEffects"]>[0];

const recoveryLayer = (input: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly plan?: DurableReattach.DurableReattachPlan["Service"];
  readonly commits: Array<CommitInput>;
  readonly writes: Array<WriteInput>;
  readonly cancelUnsettled?: EffectOutbox.EffectOutboxV2["Service"]["cancelUnsettled"];
}) =>
  ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(input.projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          commitCommand: (commit) => {
            input.commits.push(commit);
            return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
          },
          writeWithEffects: (write) => {
            input.writes.push(write);
            return Effect.succeed([]);
          },
        }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          cancelUnsettled: input.cancelUnsettled ?? (() => Effect.succeed([])),
          signalCancellations: () => Effect.void,
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
    Layer.provide(
      input.plan === undefined
        ? Layer.empty
        : Layer.succeed(DurableReattach.DurableReattachPlan, input.plan),
    ),
  );

const runEvents = (commit: CommitInput | undefined, runId: RunId) =>
  (commit?.events ?? []).filter((event) => event.type === "run.updated" && event.runId === runId);

it.effect("startup keeps a durable run, plans its conversation and enqueues its reattach", () =>
  Effect.gen(function* () {
    const plan = yield* DurableReattach.make;
    const commits: Array<CommitInput> = [];
    const writes: Array<WriteInput> = [];
    const cancelUnsettled = vi.fn<EffectOutbox.EffectOutboxV2["Service"]["cancelUnsettled"]>(() =>
      Effect.succeed([]),
    );
    yield* Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    }).pipe(
      Effect.provide(
        recoveryLayer({
          projection: recoveryProjection({ withCodexRun: true }),
          plan,
          commits,
          writes,
          cancelUnsettled,
        }),
      ),
    );
    const commit = commits[0];
    // The stock provider's run is cancelled as before; the durable one is untouched.
    assert.deepEqual(
      runEvents(commit, codexRun).map((event) =>
        event.type === "run.updated" ? event.payload.status : null,
      ),
      ["cancelled"],
    );
    assert.lengthOf(runEvents(commit, durableRun), 0);
    const touched = (commit?.events ?? []).flatMap((event): ReadonlyArray<string> =>
      event.type === "turn-item.updated" ||
      event.type === "provider-thread.updated" ||
      event.type === "provider-turn.updated" ||
      event.type === "run-attempt.updated"
        ? [event.payload.id]
        : [],
    );
    assert.notInclude(touched, TurnItemId.make("turn_item_durable_tool"));
    assert.notInclude(touched, durableThread);
    assert.notInclude(touched, durableAttempt);
    assert.include(touched, codexThread);
    // One reattach, after any left over from an earlier startup is cancelled.
    const reattaches = (commit?.effects ?? []).filter(
      (effect) => effect.request.type === "durable-run.reattach",
    );
    assert.deepEqual(
      reattaches.map((effect) => effect.request),
      [{ type: "durable-run.reattach", runId: durableRun }],
    );
    assert.isTrue(
      cancelUnsettled.mock.calls.some(([call]) =>
        call.effectTypes.includes("durable-run.reattach"),
      ),
    );
    assert.deepEqual(yield* plan.conversationsFor(durableInstance), [
      { storeId: STORE_ID, conversationId: 7 },
    ]);
    assert.deepEqual(yield* plan.conversationsFor(codexInstance), []);
  }),
);

it.effect("a thread whose only live work is a durable run still gets its reattach", () =>
  Effect.gen(function* () {
    const plan = yield* DurableReattach.make;
    const commits: Array<CommitInput> = [];
    const writes: Array<WriteInput> = [];
    yield* Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    }).pipe(
      Effect.provide(
        recoveryLayer({
          projection: recoveryProjection({ withCodexRun: false }),
          plan,
          commits,
          writes,
        }),
      ),
    );
    assert.lengthOf(commits, 0, "nothing to cancel, so no reconcile command");
    assert.deepEqual(
      writes.flatMap((write) => write.effects.map((effect) => effect.request)),
      [{ type: "durable-run.reattach", runId: durableRun }],
    );
  }),
);

it.effect("shutdown leaves a durable run running for the next startup to reattach", () =>
  Effect.gen(function* () {
    const plan = yield* DurableReattach.make;
    const commits: Array<CommitInput> = [];
    const writes: Array<WriteInput> = [];
    yield* Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("shutdown");
    }).pipe(
      Effect.provide(
        recoveryLayer({
          projection: recoveryProjection({ withCodexRun: true }),
          plan,
          commits,
          writes,
        }),
      ),
    );
    const commit = commits[0];
    assert.lengthOf(runEvents(commit, durableRun), 0);
    assert.lengthOf(runEvents(commit, codexRun), 1);
    assert.deepEqual(
      [...(commit?.effects ?? []), ...writes.flatMap((write) => write.effects)].filter(
        (effect) => effect.request.type === "durable-run.reattach",
      ),
      [],
      "only startup reattaches",
    );
  }),
);

it.effect("without a reattach plan a durable run is cancelled as stock T3 does", () =>
  Effect.gen(function* () {
    const commits: Array<CommitInput> = [];
    yield* Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    }).pipe(
      Effect.provide(
        recoveryLayer({
          projection: recoveryProjection({ withCodexRun: false }),
          commits,
          writes: [],
        }),
      ),
    );
    assert.deepEqual(
      runEvents(commits[0], durableRun).map((event) =>
        event.type === "run.updated" ? event.payload.status : null,
      ),
      ["cancelled"],
    );
  }),
);

it.effect("a running run is durable-bound while its provider turn's submission was admitted", () =>
  Effect.sync(() => {
    const projection = recoveryProjection({ withCodexRun: true });
    const withTurnStatus = (
      status: OrchestrationV2ThreadProjection["providerTurns"][number]["status"],
    ) =>
      DurableReattach.durableBoundRuns({
        ...projection,
        providerTurns: projection.providerTurns.map((turn) => ({ ...turn, status })),
      }).map(({ run }) => run.id);
    assert.deepEqual(withTurnStatus("running"), [durableRun]);
    // Settled just before the crash, with the run not yet finalized.
    assert.deepEqual(withTurnStatus("completed"), [durableRun]);
    assert.deepEqual(withTurnStatus("interrupted"), [durableRun]);
    // A failed turn may never have admitted its input.
    assert.deepEqual(withTurnStatus("failed"), []);
  }),
);

it.effect("a failed reattach settles the run's open output and leaves the rest", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const rootNodeId = "node_root";
    const projection = {
      messages: [
        { id: "m_streaming", runId: durableRun, nodeId: "node_answer", streaming: true },
        { id: "m_done", runId: durableRun, nodeId: "node_old", streaming: false },
        { id: "m_other", runId: codexRun, nodeId: null, streaming: true },
      ],
      turnItems: [
        { id: "i_answer", runId: durableRun, type: "assistant_message", status: "running" },
        { id: "i_tool", runId: durableRun, type: "dynamic_tool", status: "running" },
        { id: "i_done", runId: durableRun, type: "dynamic_tool", status: "completed" },
      ],
      nodes: [
        { id: rootNodeId, runId: durableRun, status: "running" },
        { id: "node_answer", runId: durableRun, status: "running" },
        { id: "node_old", runId: durableRun, status: "completed" },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const settled = DurableReattach.settledOpenRunOutput({
      runId: durableRun,
      rootNodeId,
      projection,
      now,
    });
    assert.deepEqual(
      settled.messages.map((message) => [message.id, message.streaming]),
      [["m_streaming", false]],
    );
    assert.deepEqual(
      settled.turnItems.map((item): ReadonlyArray<unknown> => [
        item.id,
        item.status,
        "streaming" in item ? item.streaming : undefined,
      ]),
      [
        ["i_answer", "cancelled", false],
        ["i_tool", "cancelled", undefined],
      ],
    );
    assert.deepEqual(
      settled.nodes.map((node) => [node.id, node.status]),
      [["node_answer", "cancelled"]],
      "the root node is settled with the run",
    );
  }),
);

it.effect("a failed provider turn is adoptable only once its input was admitted", () =>
  Effect.sync(() => {
    const projection = recoveryProjection({ withCodexRun: false });
    const withFailedTurn = (nativeTurnRef: unknown) =>
      DurableReattach.durableBoundRuns({
        ...projection,
        providerTurns: projection.providerTurns.map((turn) => ({
          ...turn,
          status: "failed" as const,
          nativeTurnRef: nativeTurnRef as never,
        })),
      }).map(({ run }) => run.id);
    // The adapter sets the submission ref when the turn's input settles.
    assert.deepEqual(
      withFailedTurn({
        driver: "pi",
        nativeId: `pi-durable-submission:${STORE_ID}:5`,
        strength: "strong",
      }),
      [durableRun],
    );
    // A turn refused before admission keeps its weak synthetic ref.
    assert.deepEqual(
      withFailedTurn({ driver: "pi", nativeId: "thread:attempt:1", strength: "weak" }),
      [],
    );
  }),
);

it.effect("the plan stops kept work through the instance that registered", () =>
  Effect.gen(function* () {
    const plan = yield* DurableReattach.make;
    const stopped: Array<number> = [];
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* plan.registerAbandon(durableInstance, (conversation) =>
          Effect.sync(() => stopped.push(conversation.conversationId)),
        );
        yield* plan.abandon(durableInstance, { storeId: STORE_ID, conversationId: 7 });
        yield* plan.abandon(codexInstance, { storeId: STORE_ID, conversationId: 8 });
      }),
    );
    // The registration ends with the instance.
    yield* plan.abandon(durableInstance, { storeId: STORE_ID, conversationId: 9 });
    assert.deepEqual(stopped, [7]);
  }),
);
