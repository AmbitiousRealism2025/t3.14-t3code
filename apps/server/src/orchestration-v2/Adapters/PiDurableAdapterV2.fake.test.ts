/**
 * PiDurableAdapterV2 against a scripted in-process worker, for orderings a
 * real worker cannot produce on demand: settlement racing a steer, rejected
 * admission, attachments, thread rebinding and reattaching after a restart.
 */
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ChatAttachment,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy, type ProviderAdapterV2Event } from "../ProviderAdapter.ts";
import {
  isTransientDurableFailure,
  makePiDurableAdapterV2,
  PiDurableWorkerError,
  type DurableWorker,
} from "./PiDurableAdapterV2.ts";
import type { PiRpcRecord } from "./PiRpc.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-durable-fake-",
}).pipe(Layer.provide(NodeServices.layer));
const testLayer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, serverConfigLayer);

const INSTANCE_ID = ProviderInstanceId.make("pi-durable");
const STORE_ID = "00000000-0000-4000-8000-00000000000a";
const CONVERSATION_ID = 7;
const THREAD_ID = ThreadId.make("thread-durable-fake");
const MODEL = "faux/faux-1";
const policy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/tmp",
});

type Handler = (params: PiRpcRecord) => Effect.Effect<unknown, PiDurableWorkerError>;

/** A worker whose requests are answered by `handlers` and whose events the test pushes. */
const makeFakeWorker = (handlers: Record<string, Handler>) =>
  Effect.sync(() => {
    const subscribers: Array<Queue.Queue<PiRpcRecord, PiDurableWorkerError>> = [];
    const requests: Array<{ readonly method: string; readonly params: PiRpcRecord }> = [];
    const worker: DurableWorker = {
      status: { storeId: STORE_ID, ownerEpoch: 1, scheduling: "enabled", reasons: [] },
      currentStatus: Effect.succeed({
        storeId: STORE_ID,
        ownerEpoch: 1,
        scheduling: "enabled",
        reasons: [],
      }),
      defaultModel: { provider: "faux", modelId: "faux-1" },
      tools: "read",
      models: [
        {
          provider: "faux",
          modelId: "faux-1",
          name: "Fixture",
          contextWindow: 1000,
          reasoning: false,
        },
      ],
      // Logged when the request runs, not when its effect is built.
      request: (method, params = {}) =>
        Effect.suspend(() => {
          requests.push({ method, params });
          const handler = handlers[method];
          return handler === undefined ? Effect.succeed({}) : handler(params);
        }),
      subscribe: () =>
        Effect.acquireRelease(
          Effect.gen(function* () {
            const queue = yield* Queue.unbounded<PiRpcRecord, PiDurableWorkerError>();
            subscribers.push(queue);
            return queue;
          }),
          (queue) =>
            Effect.sync(() => {
              subscribers.splice(subscribers.indexOf(queue), 1);
            }),
        ),
      isAlive: () => true,
    };
    /** Deliver one batch to the live watch. */
    const push = (...events: ReadonlyArray<PiRpcRecord>) =>
      Effect.forEach(
        subscribers,
        (queue) => Queue.offer(queue, { type: "events", conversationId: CONVERSATION_ID, events }),
        { discard: true },
      );
    /**
     * Resolves once the adapter has taken every pushed batch. While nothing
     * holds the adapter's permit it handles a batch as soon as it takes it.
     */
    const drained: Effect.Effect<void> = Effect.gen(function* () {
      while (true) {
        const sizes = yield* Effect.forEach(subscribers, (queue) => Queue.size(queue));
        if (sizes.every((size) => size === 0)) return;
        yield* Effect.yieldNow;
      }
    });
    return { worker, push, drained, requests, subscriberCount: () => subscribers.length };
  });

const defaultHandlers: Record<string, Handler> = {
  "conversation.open": () =>
    Effect.succeed({
      conversationId: CONVERSATION_ID,
      nativeThreadRef: `pi-durable:${STORE_ID}:${CONVERSATION_ID}`,
      created: true,
    }),
  "conversation.watch": () => Effect.succeed({ snapshot: {} }),
};

const openRuntime = Effect.fnUntraced(function* (worker: DurableWorker, model = MODEL) {
  const adapter = makePiDurableAdapterV2({
    instanceId: INSTANCE_ID,
    workers: { get: Effect.succeed(worker) },
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: ProviderSessionId.make("session-fake"),
    modelSelection: { instanceId: INSTANCE_ID, model },
    runtimePolicy: policy,
  });
  const seen: ProviderAdapterV2Event[] = [];
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => seen.push(event)).pipe(Effect.andThen(Queue.offer(emitted, event))),
    ),
    Effect.forkScoped,
  );
  /** The next emitted event matching `predicate`; the deadline only reports a hang. */
  const next = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    }).pipe(Effect.timeout("10 seconds"));
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: { instanceId: INSTANCE_ID, model },
    runtimePolicy: policy,
  });
  return { runtime, seen, next, providerThread };
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  attachments: ReadonlyArray<ChatAttachment> = [],
) {
  const now = yield* DateTime.now;
  const runId = RunId.make(`run:${THREAD_ID}:1`);
  return {
    appThread: {
      createdBy: "user",
      creationSource: "web",
      id: THREAD_ID,
      projectId: "project:fixture:durable-fake" as OrchestrationV2AppThread["projectId"],
      title: "Durable fake thread",
      providerInstanceId: INSTANCE_ID,
      modelSelection: { instanceId: INSTANCE_ID, model: MODEL },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    } satisfies OrchestrationV2AppThread,
    threadId: THREAD_ID,
    runId,
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:1` as never,
      text: "hello",
      attachments,
      createdBy: "user" as const,
      creationSource: "web" as const,
    },
    modelSelection: { instanceId: INSTANCE_ID, model: MODEL },
    runtimePolicy: policy,
  };
});

const settled = (id: number) => ({
  type: "submission",
  record: { id, status: "done", answer: id * 10 },
});

const isTerminal = (event: ProviderAdapterV2Event) => event.type === "turn.terminal";

/** An assistant partial: observable output only while the turn is still active. */
const marker = (text: string) => ({
  type: "message_start",
  message: { role: "assistant", content: [{ type: "text", text }] },
});
const isMarker = (text: string) => (event: ProviderAdapterV2Event) =>
  event.type === "message.updated" && event.message.text === text;

describe("PiDurableAdapterV2 (scripted worker)", () => {
  it.live("a steer admitted while the first submission settles keeps the turn open", () =>
    Effect.gen(function* () {
      const steerAnswer = yield* Deferred.make<unknown>();
      const steerSent = yield* Deferred.make<void>();
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => {
          submits += 1;
          return submits === 1
            ? Effect.succeed({ submissionId: 1, status: "placed" })
            : Deferred.succeed(steerSent, undefined).pipe(
                Effect.andThen(Deferred.await(steerAnswer)),
              );
        },
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const input = yield* turnInput(providerThread);
      yield* runtime.startTurn(input);
      const running = yield* next(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      const steering = yield* runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread,
          providerTurnId: running.providerTurn.id,
          message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(steerSent);

      // The first submission settles while the steer RPC is still in flight.
      // Events in a batch are handled in order, so the marker shows whether the
      // turn was still active after the settlement.
      yield* fake.push(settled(1), marker("after settlement"));
      const first = yield* next(
        (event) => isTerminal(event) || isMarker("after settlement")(event),
      );
      assert.isFalse(isTerminal(first), "the turn must wait for the steer's admission");

      yield* Deferred.succeed(steerAnswer, { submissionId: 2, status: "queued" });
      yield* Fiber.join(steering);
      yield* fake.push(marker("after admission"));
      const second = yield* next(
        (event) => isTerminal(event) || isMarker("after admission")(event),
      );
      assert.isFalse(isTerminal(second), "the steer's submission is still unsettled");

      yield* fake.push(settled(2));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a steer racing the settlement is either refused or keeps the turn open", () =>
    Effect.gen(function* () {
      // Fiber scheduling decides which side wins each round; the invariant
      // must hold for every interleaving.
      for (let round = 0; round < 40; round += 1) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            let submits = 0;
            const fake = yield* makeFakeWorker({
              ...defaultHandlers,
              "conversation.submit": () => {
                submits += 1;
                return Effect.succeed({ submissionId: submits, status: "placed" });
              },
            });
            const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
            const input = yield* turnInput(providerThread);
            yield* runtime.startTurn(input);
            const running = yield* next(
              (event) =>
                event.type === "provider_turn.updated" && event.providerTurn.status === "running",
            );
            if (running.type !== "provider_turn.updated") return assert.fail("no running turn");
            const steer = runtime
              .steerTurn({
                threadId: THREAD_ID,
                runId: input.runId,
                providerThread,
                providerTurnId: running.providerTurn.id,
                message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
              })
              .pipe(Effect.exit);
            const [steered] = yield* Effect.all([steer, fake.push(settled(1))], {
              concurrency: "unbounded",
            });
            if (steered._tag === "Failure") {
              // Refused: the turn had already ended on submission 1.
              const ended = yield* next(isTerminal);
              assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
              assert.strictEqual(submits, 1, "a refused steer admits nothing");
              return;
            }
            // Admitted: the turn must wait for the steer's own submission.
            yield* fake.push(marker(`round ${round}`));
            const first = yield* next(
              (event) => isTerminal(event) || isMarker(`round ${round}`)(event),
            );
            assert.isFalse(isTerminal(first), `round ${round}: an admitted steer was orphaned`);
            yield* fake.push(settled(2));
            const ended = yield* next(isTerminal);
            assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
          }),
        );
      }
    }).pipe(Effect.provide(testLayer)),
  );

  it.live("work resumed from before a restart stays out of the next turn", () =>
    Effect.gen(function* () {
      // The store resumed submission 99 at boot and has 98 queued behind it;
      // no T3 turn owns either.
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () =>
          Effect.succeed({
            snapshot: { run: { inputs: [99] }, inbox: [{ id: 98, mode: "followUp" }] },
          }),
        "conversation.submit": () => Effect.succeed({ submissionId: 1, status: "queued" }),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn(yield* turnInput(providerThread));
      yield* fake.push(
        marker("resumed answer"),
        { type: "run_end", inputs: [99] },
        { type: "submission", record: { id: 99, status: "done", answer: 990 } },
        { type: "run_start", inputs: [98] },
      );
      yield* fake.push(
        marker("queued answer"),
        { type: "run_end", inputs: [98] },
        { type: "submission", record: { id: 98, status: "done", answer: 980 } },
        { type: "run_start", inputs: [1] },
      );
      yield* fake.push(marker("our answer"), settled(1), { type: "run_end", inputs: [1] });
      const first = yield* next(
        (event) => event.type === "message.updated" || event.type === "turn.terminal",
      );
      assert.isTrue(
        first.type === "message.updated" && first.message.text === "our answer",
        "the first output in the turn is its own",
      );
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a steer during resumed work queues behind the turn instead of steering it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () => Effect.succeed({ snapshot: { run: { inputs: [99] } } }),
        "conversation.submit": (params) =>
          Effect.succeed({
            submissionId: params["whenBusy"] === "followUp" ? 1 : 2,
            status: "queued",
          }),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const input = yield* turnInput(providerThread);
      yield* runtime.startTurn(input);
      const running = yield* next(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      if (running.type !== "provider_turn.updated") return assert.fail("no running turn");
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread,
        providerTurnId: running.providerTurn.id,
        message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
      });
      const modes = fake.requests.flatMap((request) =>
        request.method === "conversation.submit" ? [request.params["whenBusy"]] : [],
      );
      assert.deepStrictEqual(modes, ["followUp", "followUp"]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("an admission a rebind saw as foreign still settles its turn", () =>
    Effect.gen(function* () {
      const submitAnswer = yield* Deferred.make<unknown>();
      const submitSent = yield* Deferred.make<void>();
      let watches = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        // The second watch (the rebind) already sees submission 1 running.
        "conversation.watch": () => {
          watches += 1;
          return Effect.succeed({ snapshot: watches === 1 ? {} : { run: { inputs: [1] } } });
        },
        "conversation.submit": () =>
          Deferred.succeed(submitSent, undefined).pipe(
            Effect.andThen(Deferred.await(submitAnswer)),
          ),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const starting = yield* runtime
        .startTurn(yield* turnInput(providerThread))
        .pipe(Effect.forkScoped);
      yield* Deferred.await(submitSent);
      yield* runtime.resumeThread({ providerThread });
      // The answer settles before the submit call returns its ID.
      yield* fake.push(settled(1), { type: "run_end", inputs: [1] });
      yield* Deferred.succeed(submitAnswer, { submissionId: 1, status: "placed" });
      yield* Fiber.join(starting);
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a submit whose reply was lost is asked again and keeps its submission", () =>
    Effect.gen(function* () {
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        // The first reply never arrives (the worker admitted it anyway); the
        // same request ID then returns the original submission.
        "conversation.submit": () => {
          submits += 1;
          return submits === 1
            ? Effect.fail(
                new PiDurableWorkerError({ detail: "Pi RPC conversation.submit timed out" }),
              )
            : Effect.succeed({ submissionId: 1, status: "placed" });
        },
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn(yield* turnInput(providerThread));
      const requestIds = fake.requests.flatMap((request) =>
        request.method === "conversation.submit" ? [request.params["requestId"]] : [],
      );
      assert.lengthOf(requestIds, 2);
      assert.strictEqual(requestIds[0], requestIds[1], "the retry reuses the request ID");
      yield* fake.push(settled(1));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("Stop during an in-flight steer aborts the steer's submission too", () =>
    Effect.gen(function* () {
      const steerAnswer = yield* Deferred.make<unknown>();
      const steerSent = yield* Deferred.make<void>();
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => {
          submits += 1;
          return submits === 1
            ? Effect.succeed({ submissionId: 1, status: "placed" })
            : Deferred.succeed(steerSent, undefined).pipe(
                Effect.andThen(Deferred.await(steerAnswer)),
              );
        },
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const input = yield* turnInput(providerThread);
      yield* runtime.startTurn(input);
      const running = yield* next(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      if (running.type !== "provider_turn.updated") return assert.fail("no running turn");
      const steering = yield* runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread,
          providerTurnId: running.providerTurn.id,
          message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(steerSent);
      // Stop reaches the worker before the steer's submission exists.
      yield* runtime.interruptTurn({ providerThread, providerTurnId: running.providerTurn.id });
      yield* Deferred.succeed(steerAnswer, { submissionId: 2, status: "queued" });
      yield* Fiber.join(steering);
      const order = fake.requests.flatMap((request) =>
        request.method === "conversation.submit" || request.method === "conversation.abort"
          ? [request.method]
          : [],
      );
      assert.deepStrictEqual(order, [
        "conversation.submit",
        "conversation.submit",
        "conversation.abort",
        "conversation.abort",
      ]);
      // A steer after Stop admits nothing.
      const late = yield* runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread,
          providerTurnId: running.providerTurn.id,
          message: { ...input.message, messageId: `message:${THREAD_ID}:3` as never },
        })
        .pipe(Effect.flip);
      assert.strictEqual(late._tag, "ProviderAdapterSteerRunError");
      assert.strictEqual(submits, 2);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("rebinding the watched conversation keeps its watch", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker(defaultHandlers);
      const { runtime, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.resumeThread({ providerThread });
      const watchCalls = fake.requests.filter(
        (request) =>
          request.method === "conversation.watch" || request.method === "conversation.unwatch",
      );
      assert.deepStrictEqual(
        watchCalls.map((request) => request.method),
        ["conversation.watch"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a Stop for a turn that already ended leaves the next turn running", () =>
    Effect.gen(function* () {
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => {
          submits += 1;
          return Effect.succeed({ submissionId: submits, status: "placed" });
        },
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const first = yield* turnInput(providerThread);
      yield* runtime.startTurn(first);
      const firstRunning = yield* next(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      if (firstRunning.type !== "provider_turn.updated") return assert.fail("no running turn");
      yield* fake.push(settled(1));
      yield* next(isTerminal);
      const second = yield* turnInput(providerThread);
      yield* runtime.startTurn({
        ...second,
        runId: RunId.make(`run:${THREAD_ID}:2`),
        runOrdinal: 2,
        providerTurnOrdinal: 2,
        attemptId: RunAttemptId.make(`run-attempt:run:${THREAD_ID}:2:1`),
        message: { ...second.message, messageId: `message:${THREAD_ID}:2` as never },
      });
      // The late Stop still names the first turn.
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: firstRunning.providerTurn.id,
        requestRuntimeRestart: true,
      });
      assert.isFalse(fake.requests.some((request) => request.method === "conversation.abort"));
      yield* fake.push(settled(2));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a rebind during a running turn keeps the provider thread active", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => Effect.succeed({ submissionId: 1, status: "placed" }),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn(yield* turnInput(providerThread));
      yield* next(
        (event) =>
          event.type === "provider_thread.updated" && event.providerThread.status === "active",
      );
      const rebound = yield* runtime.resumeThread({ providerThread });
      assert.strictEqual(rebound.status, "active");
      const emitted = yield* next((event) => event.type === "provider_thread.updated");
      assert.isTrue(
        emitted.type === "provider_thread.updated" && emitted.providerThread.status === "active",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a rejected initial submission closes the running provider turn as failed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () =>
          Effect.fail(
            new PiDurableWorkerError({
              detail: "scheduling is inhibited",
              errorName: "SchedulingInhibited",
            }),
          ),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      const error = yield* runtime.startTurn(yield* turnInput(providerThread)).pipe(Effect.flip);
      assert.strictEqual(error._tag, "ProviderAdapterTurnStartError");
      const isTurnUpdate = (event: ProviderAdapterV2Event) =>
        event.type === "provider_turn.updated";
      const updates = [yield* next(isTurnUpdate), yield* next(isTurnUpdate)];
      assert.deepStrictEqual(
        updates.map((event) =>
          event.type === "provider_turn.updated" ? event.providerTurn.status : null,
        ),
        ["running", "failed"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a message with attachments is refused instead of silently dropping them", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker(defaultHandlers);
      const { runtime, providerThread } = yield* openRuntime(fake.worker);
      const attachment = {
        type: "image",
        id: "attachment-1",
        name: "screenshot.png",
        mimeType: "image/png",
        sizeBytes: 10,
      } as unknown as ChatAttachment;
      const error = yield* runtime
        .startTurn(yield* turnInput(providerThread, [attachment]))
        .pipe(Effect.flip);
      assert.strictEqual(error._tag, "ProviderAdapterTurnStartError");
      assert.isFalse(
        fake.requests.some((request) => request.method === "conversation.submit"),
        "nothing is admitted",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("Pi's default selection opens the conversation on the worker's default model", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker(defaultHandlers);
      yield* openRuntime(fake.worker, "default");
      const open = fake.requests.find((request) => request.method === "conversation.open");
      assert.deepStrictEqual(open?.params["model"], { provider: "faux", modelId: "faux-1" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("rebinding the thread keeps the session's event stream alive", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => Effect.succeed({ submissionId: 1, status: "placed" }),
      });
      const { runtime, seen, next, providerThread } = yield* openRuntime(fake.worker);
      // A model or policy change makes the session manager resume the thread again.
      const rebound = yield* runtime.resumeThread({ providerThread });
      assert.strictEqual(fake.subscriberCount(), 1);
      yield* runtime.startTurn(yield* turnInput(rebound));
      yield* fake.push(settled(1));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
      // A wrongly failed stream would have reported the error before the terminal.
      assert.isFalse(
        seen.some(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "error",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattached turn adopts its running submission and the output so far", () =>
    Effect.gen(function* () {
      const statusAsked = yield* Deferred.make<void>();
      const adopt = yield* Deferred.make<void>();
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        // Submission 5 (entry 10) ran before the restart: one answer with a
        // tool call, its result, and a second answer still streaming.
        "conversation.watch": () =>
          Effect.succeed({
            snapshot: {
              entries: [
                { id: 10, model: [{ role: "user", content: "hello" }] },
                {
                  id: 11,
                  model: [
                    {
                      role: "assistant",
                      content: [
                        { type: "text", text: "Reading it." },
                        { type: "toolCall", id: "call-1", name: "read", arguments: { path: "a" } },
                      ],
                    },
                  ],
                },
                {
                  id: 12,
                  model: [
                    {
                      role: "toolResult",
                      toolCallId: "call-1",
                      toolName: "read",
                      content: [{ type: "text", text: "file body" }],
                      isError: false,
                    },
                  ],
                },
              ],
              run: { inputs: [5] },
              generation: {
                attempt: 1,
                message: { role: "assistant", content: [{ type: "text", text: "The file" }] },
              },
              tools: [],
              inbox: [],
            },
          }),
        "conversation.submit": () => {
          submits += 1;
          return Effect.succeed(
            submits === 1
              ? { submissionId: 5, status: "placed" }
              : { submissionId: 6, status: "queued" },
          );
        },
        "submission.status": () =>
          Deferred.succeed(statusAsked, undefined).pipe(
            Effect.andThen(Deferred.await(adopt)),
            Effect.as({ record: { id: 5, status: "placed", entry: 10 } }),
          ),
      });
      const { runtime, seen, next, providerThread } = yield* openRuntime(fake.worker);
      const input = yield* turnInput(providerThread);
      const starting = yield* runtime
        .startTurn({ ...input, reattach: { startedAt: null } })
        .pipe(Effect.forkScoped);
      // The run streams more before the turn has adopted it.
      yield* Deferred.await(statusAsked);
      yield* fake.push({
        type: "message_update",
        changes: [{ type: "text_delta", contentIndex: 0, delta: " says" }],
      });
      yield* fake.drained;
      yield* Deferred.succeed(adopt, undefined);
      yield* Fiber.join(starting);
      yield* fake.push(
        {
          type: "message_update",
          changes: [{ type: "text_delta", contentIndex: 0, delta: " hi" }],
        },
        marker("adopted"),
      );
      yield* next(isMarker("adopted"));
      const running = seen.find(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      if (running?.type !== "provider_turn.updated") return assert.fail("no running turn");
      // The run is the turn's own now, so a message steers it.
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread,
        providerTurnId: running.providerTurn.id,
        message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
      });
      yield* fake.push(settled(5), settled(6));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");

      const submitted = fake.requests.flatMap((request) =>
        request.method === "conversation.submit" ? [request.params] : [],
      );
      assert.deepStrictEqual(
        submitted.map((params) => [params["requestId"], params["whenBusy"]]),
        [
          [`t3:${THREAD_ID}:message:${THREAD_ID}:1`, "followUp"],
          [`t3:${THREAD_ID}:message:${THREAD_ID}:2`, "steer"],
        ],
      );
      assert.isFalse(
        fake.requests.some((request) => request.method === "conversation.configure"),
        "the run keeps the selection it started with",
      );
      const messages = new Map<string, string>();
      const tools = new Map<string, string>();
      for (const event of seen) {
        if (event.type === "message.updated") messages.set(event.message.id, event.message.text);
        if (event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool") {
          tools.set(event.turnItem.id, `${event.turnItem.status}:${event.turnItem.output ?? ""}`);
        }
      }
      assert.deepStrictEqual(Array.from(messages.values()), [
        "Reading it.",
        "The file says hi",
        "adopted",
      ]);
      assert.deepStrictEqual(Array.from(tools.values()), ["completed:file body"]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattached submission that settled before the watch ends the turn at once", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () =>
          Effect.succeed({
            snapshot: {
              entries: [
                { id: 10, model: [{ role: "user", content: "hello" }] },
                {
                  id: 11,
                  model: [
                    {
                      role: "assistant",
                      content: [{ type: "text", text: "the answer" }],
                      stopReason: "stop",
                    },
                  ],
                },
              ],
              tools: [],
              inbox: [],
            },
          }),
        "conversation.submit": () => Effect.succeed({ submissionId: 5, status: "done" }),
        "submission.status": () =>
          Effect.succeed({ record: { id: 5, status: "done", entry: 10, answer: 11 } }),
      });
      const { runtime, seen, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn({
        ...(yield* turnInput(providerThread)),
        reattach: { startedAt: null },
      });
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
      assert.deepStrictEqual(
        seen.flatMap((event) =>
          event.type === "message.updated" && !event.message.streaming ? [event.message.text] : [],
        ),
        ["the answer"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattach adopts coding work whatever runtime mode the thread has now", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () => Effect.succeed({ snapshot: { run: { inputs: [5] } } }),
        "conversation.submit": () => Effect.succeed({ submissionId: 5, status: "placed" }),
        "submission.status": () =>
          Effect.succeed({ record: { id: 5, status: "placed", entry: 10 } }),
      });
      const { runtime, next, providerThread } = yield* openRuntime({
        ...fake.worker,
        tools: "coding",
      });
      // The run started in Full access; the thread was switched afterwards.
      yield* runtime.startTurn({
        ...(yield* turnInput(providerThread)),
        runtimePolicy: ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "approval-required",
          interactionMode: "default",
          cwd: "/tmp",
        }),
        reattach: { startedAt: null },
      });
      yield* fake.push(settled(5));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattach that cannot adopt the kept work stops it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () => Effect.succeed({ snapshot: { run: { inputs: [5] } } }),
        "conversation.submit": () =>
          Effect.fail(new PiDurableWorkerError({ detail: "refused", errorName: "Refused" })),
      });
      const { runtime, providerThread } = yield* openRuntime(fake.worker);
      const error = yield* runtime
        .startTurn({ ...(yield* turnInput(providerThread)), reattach: { startedAt: null } })
        .pipe(Effect.flip);
      assert.strictEqual(error._tag, "ProviderAdapterTurnStartError");
      assert.deepStrictEqual(
        fake.requests.flatMap((request) =>
          request.method === "conversation.abort" ? [request.params["conversationId"]] : [],
        ),
        [CONVERSATION_ID],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a final failed reattach resume stops the kept work; an earlier one retries", () =>
    Effect.gen(function* () {
      let reopenFailure: PiDurableWorkerError = new PiDurableWorkerError({
        detail: "Pi RPC conversation.open timed out",
      });
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        // Opening the existing conversation fails; creating one works.
        "conversation.open": (params) =>
          params["conversationId"] === undefined
            ? defaultHandlers["conversation.open"]!(params)
            : Effect.fail(reopenFailure),
      });
      const { runtime, providerThread } = yield* openRuntime(fake.worker);
      const aborts = () =>
        fake.requests.filter((request) => request.method === "conversation.abort").length;

      const retried = yield* runtime
        .resumeThread({ providerThread, reattach: { finalAttempt: false } })
        .pipe(Effect.flip);
      assert.isTrue(isTransientDurableFailure(retried), "no worker answer: worth retrying");
      assert.strictEqual(aborts(), 0, "a retry will still adopt the work");

      yield* runtime
        .resumeThread({ providerThread, reattach: { finalAttempt: true } })
        .pipe(Effect.flip);
      assert.deepStrictEqual(
        fake.requests.flatMap((request) =>
          request.method === "conversation.abort" ? [request.params["conversationId"]] : [],
        ),
        [CONVERSATION_ID],
      );

      reopenFailure = new PiDurableWorkerError({
        detail: "conversation 7 does not exist",
        errorName: "ConversationNotFoundError",
      });
      const missing = yield* runtime
        .resumeThread({ providerThread, reattach: { finalAttempt: false } })
        .pipe(Effect.flip);
      assert.isFalse(isTransientDurableFailure(missing), "a missing conversation stays missing");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattached turn waits for the steers accepted before the restart", () =>
    Effect.gen(function* () {
      // Submission 6 steered the running run; 7 was queued behind it.
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () =>
          Effect.succeed({
            snapshot: { run: { inputs: [5, 6] }, inbox: [{ id: 7, mode: "followUp" }] },
          }),
        "conversation.submit": () => Effect.succeed({ submissionId: 5, status: "placed" }),
        "submission.status": () =>
          Effect.succeed({ record: { id: 5, status: "placed", entry: 10 } }),
      });
      const { runtime, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn({
        ...(yield* turnInput(providerThread)),
        reattach: { startedAt: null },
      });
      yield* fake.push(
        settled(5),
        settled(6),
        { type: "run_end", inputs: [5, 6] },
        { type: "run_start", inputs: [7] },
        marker("the queued steer's answer"),
      );
      const first = yield* next(
        (event) => isTerminal(event) || isMarker("the queued steer's answer")(event),
      );
      assert.isFalse(isTerminal(first), "the turn waits for every steer and keeps their output");
      yield* fake.push(settled(7), { type: "run_end", inputs: [7] });
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a steer that settles while the reattach is adopting stays the turn's", () =>
    Effect.gen(function* () {
      const statusAsked = yield* Deferred.make<void>();
      const adopt = yield* Deferred.make<void>();
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () =>
          Effect.succeed({
            snapshot: { run: { inputs: [5] }, inbox: [{ id: 7, mode: "followUp" }] },
          }),
        "conversation.submit": () => Effect.succeed({ submissionId: 5, status: "placed" }),
        "submission.status": () =>
          Deferred.succeed(statusAsked, undefined).pipe(
            Effect.andThen(Deferred.await(adopt)),
            Effect.as({ record: { id: 5, status: "placed", entry: 10 } }),
          ),
      });
      const { runtime, seen, next, providerThread } = yield* openRuntime(fake.worker);
      const starting = yield* runtime
        .startTurn({ ...(yield* turnInput(providerThread)), reattach: { startedAt: null } })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(statusAsked);
      // Both runs finish before the turn has adopted them.
      yield* fake.push(
        settled(5),
        { type: "run_end", inputs: [5] },
        { type: "run_start", inputs: [7] },
        marker("the steer's answer"),
        settled(7),
        { type: "run_end", inputs: [7] },
      );
      yield* fake.drained;
      yield* Deferred.succeed(adopt, undefined);
      yield* Fiber.join(starting);
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
      assert.isTrue(seen.some(isMarker("the steer's answer")), "the steer's output is kept");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("a reattached turn keeps its original start and retries an unanswered status read", () =>
    Effect.gen(function* () {
      const startedAt = DateTime.makeUnsafe("2026-10-05T12:00:00.000Z");
      let statusReads = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.watch": () => Effect.succeed({ snapshot: { run: { inputs: [5] } } }),
        "conversation.submit": () => Effect.succeed({ submissionId: 5, status: "placed" }),
        // The first read gets no answer; the same read is asked again.
        "submission.status": () => {
          statusReads += 1;
          return statusReads === 1
            ? Effect.fail(
                new PiDurableWorkerError({ detail: "Pi RPC submission.status timed out" }),
              )
            : Effect.succeed({ record: { id: 5, status: "placed", entry: 10 } });
        },
      });
      const { runtime, seen, next, providerThread } = yield* openRuntime(fake.worker);
      yield* runtime.startTurn({ ...(yield* turnInput(providerThread)), reattach: { startedAt } });
      yield* fake.push(settled(5));
      const ended = yield* next(isTerminal);
      assert.isTrue(ended.type === "turn.terminal" && ended.status === "completed");
      assert.strictEqual(statusReads, 2);
      const turnStarts = seen.flatMap((event) =>
        event.type === "provider_turn.updated" ? [event.providerTurn.startedAt] : [],
      );
      assert.isAbove(turnStarts.length, 1);
      assert.isTrue(
        turnStarts.every((value) => value !== null && DateTime.Equivalence(value, startedAt)),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
