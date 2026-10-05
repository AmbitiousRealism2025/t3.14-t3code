/**
 * PiDurableAdapterV2 against a scripted in-process worker, for orderings a
 * real worker cannot produce on demand: settlement racing a steer, rejected
 * admission, attachments and thread rebinding.
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
      request: (method, params = {}) => {
        requests.push({ method, params });
        const handler = handlers[method];
        return handler === undefined ? Effect.succeed({}) : handler(params);
      },
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
    return { worker, push, requests, subscriberCount: () => subscribers.length };
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
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Effect.sync(() => seen.push(event))),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: { instanceId: INSTANCE_ID, model },
    runtimePolicy: policy,
  });
  return { runtime, seen, providerThread };
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

/** Let forked fibers and queued events run. */
const settle = Effect.sleep("20 millis");

const terminals = (seen: ReadonlyArray<ProviderAdapterV2Event>) =>
  seen.filter((event) => event.type === "turn.terminal");

const runningTurnId = (seen: ReadonlyArray<ProviderAdapterV2Event>) => {
  const running = seen.find(
    (event) => event.type === "provider_turn.updated" && event.providerTurn.status === "running",
  );
  return running?.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
};

describe("PiDurableAdapterV2 (scripted worker)", () => {
  it.live("a steer admitted while the first submission settles keeps the turn open", () =>
    Effect.gen(function* () {
      const steerAnswer = yield* Deferred.make<unknown>();
      let submits = 0;
      const fake = yield* makeFakeWorker({
        ...defaultHandlers,
        "conversation.submit": () => {
          submits += 1;
          return submits === 1
            ? Effect.succeed({ submissionId: 1, status: "placed" })
            : Deferred.await(steerAnswer);
        },
      });
      const { runtime, seen, providerThread } = yield* openRuntime(fake.worker);
      const input = yield* turnInput(providerThread);
      yield* runtime.startTurn(input);
      yield* settle;
      const providerTurnId = runningTurnId(seen);
      assert.isDefined(providerTurnId);
      const steering = yield* runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread,
          providerTurnId: providerTurnId!,
          message: { ...input.message, messageId: `message:${THREAD_ID}:2` as never },
        })
        .pipe(Effect.forkScoped);
      yield* settle;

      // The first submission settles while the steer RPC is still in flight.
      yield* fake.push(settled(1));
      yield* settle;
      assert.lengthOf(terminals(seen), 0, "the turn must wait for the steer's admission");

      yield* Deferred.succeed(steerAnswer, { submissionId: 2, status: "queued" });
      yield* Fiber.join(steering);
      yield* settle;
      assert.lengthOf(terminals(seen), 0, "the steer's submission is still unsettled");

      yield* fake.push(settled(2));
      yield* settle;
      const ended = terminals(seen);
      assert.lengthOf(ended, 1);
      assert.isTrue(ended[0]?.type === "turn.terminal" && ended[0].status === "completed");
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
      const { runtime, seen, providerThread } = yield* openRuntime(fake.worker);
      const error = yield* runtime.startTurn(yield* turnInput(providerThread)).pipe(Effect.flip);
      assert.strictEqual(error._tag, "ProviderAdapterTurnStartError");
      yield* settle;
      const turnUpdates = seen.flatMap((event) =>
        event.type === "provider_turn.updated" ? [event.providerTurn.status] : [],
      );
      assert.deepStrictEqual(turnUpdates, ["running", "failed"]);
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
      const { runtime, seen, providerThread } = yield* openRuntime(fake.worker);
      // A model or policy change makes the session manager resume the thread again.
      const rebound = yield* runtime.resumeThread({ providerThread });
      yield* settle;
      assert.strictEqual(fake.subscriberCount(), 1);
      yield* runtime.startTurn(yield* turnInput(rebound));
      yield* fake.push(settled(1));
      yield* settle;
      const ended = terminals(seen);
      assert.lengthOf(ended, 1);
      assert.isTrue(ended[0]?.type === "turn.terminal" && ended[0].status === "completed");
      assert.isFalse(
        seen.some(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "error",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
