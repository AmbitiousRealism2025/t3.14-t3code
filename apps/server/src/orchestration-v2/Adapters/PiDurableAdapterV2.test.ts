/**
 * PiDurableAdapterV2 against the real T3.14 durable worker and its
 * deterministic fixture model. Runs only when `T314_WORKER_CLI` points at the
 * worker CLI (`packages/durable-host/src/cli.ts` in the T3.14 repository);
 * every worker uses a temporary data home and is stopped with its scope.
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
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import { makeDurableWorkerManager, makePiDurableAdapterV2 } from "./PiDurableAdapterV2.ts";

const WORKER_CLI = process.env.T314_WORKER_CLI;
const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-durable-adapter-",
}).pipe(Layer.provide(NodeServices.layer));
const testLayer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, serverConfigLayer);

const INSTANCE_ID = ProviderInstanceId.make("pi-durable");
const FIXTURE_MODEL = "faux/faux-1";
const runtimePolicy = (cwd: string) =>
  ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd,
  });

/** A temporary data home, workspace and witness directory, removed with the scope. */
const sandbox = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t314-adapter-" });
  return {
    workspace: dir,
    dataHome: path.join(dir, "home"),
    stateHome: path.join(dir, "state"),
    file: (name: string) => path.join(dir, name),
  };
});

const openAdapter = Effect.fnUntraced(function* (
  box: Effect.Success<typeof sandbox>,
  options: { readonly tokensPerSecond?: number; readonly tools?: string } = {},
) {
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
        options.tools ?? "read",
        "--tokens-per-second",
        String(options.tokensPerSecond ?? 0),
      ],
    },
    env: { PATH: process.env.PATH, HOME: process.env.HOME, XDG_STATE_HOME: box.stateHome },
  });
  const adapter = makePiDurableAdapterV2({
    instanceId: INSTANCE_ID,
    workers,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
  });
  return { adapter, workers };
});

const openRuntime = Effect.fnUntraced(function* (
  adapter: ReturnType<typeof makePiDurableAdapterV2>,
  workspace: string,
  sessionId = "session-1",
) {
  const runtime = yield* adapter.openSession({
    threadId: ThreadId.make("thread-durable"),
    providerSessionId: ProviderSessionId.make(sessionId),
    modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
    runtimePolicy: runtimePolicy(workspace),
  });
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const seen: ProviderAdapterV2Event[] = [];
  const takeEvent = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        seen.push(event);
        if (predicate(event)) return event;
      }
    }).pipe(Effect.timeout("20 seconds"));
  return { runtime, takeEvent, seen };
});

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  workspace: string,
  text: string,
  runOrdinal = 1,
) {
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("thread-durable");
  const appThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: "project:fixture:durable" as OrchestrationV2AppThread["projectId"],
    title: "Durable test thread",
    providerInstanceId: INSTANCE_ID,
    modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  } satisfies OrchestrationV2AppThread;
  const runId = RunId.make(`run:${threadId}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread,
    threadId,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${threadId}:${runOrdinal}` as never,
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
    runtimePolicy: runtimePolicy(workspace),
  });
});

const ensureThread = (
  runtime: ProviderAdapterV2SessionRuntime,
  workspace: string,
  existing?: OrchestrationV2ProviderThread,
) =>
  runtime.ensureThread({
    threadId: ThreadId.make("thread-durable"),
    modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
    runtimePolicy: runtimePolicy(workspace),
    ...(existing === undefined ? {} : { existingProviderThread: existing }),
  });

const assistantText = (seen: ReadonlyArray<ProviderAdapterV2Event>) =>
  seen.flatMap((event) =>
    event.type === "message.updated" && !event.message.streaming ? [event.message.text] : [],
  );

describe.skipIf(WORKER_CLI === undefined)("PiDurableAdapterV2 (real worker)", () => {
  it.live("streams a turn through the worker and completes it", () =>
    Effect.gen(function* () {
      const box = yield* sandbox;
      const { adapter } = yield* openAdapter(box);
      const { runtime, takeEvent, seen } = yield* openRuntime(adapter, box.workspace);
      const providerThread = yield* ensureThread(runtime, box.workspace);
      assert.match(providerThread.nativeThreadRef?.nativeId ?? "", /^pi-durable:[0-9a-f-]+:\d+$/);

      yield* startTurn(runtime, providerThread, box.workspace, "hello durable");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.deepStrictEqual(assistantText(seen), ["echo: hello durable"]);
      const settledTurn = seen.findLast(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      assert.isTrue(
        settledTurn?.type === "provider_turn.updated" &&
          (settledTurn.providerTurn.nativeTurnRef?.nativeId ?? "").startsWith(
            "pi-durable-submission:",
          ),
      );

      // The same thread reopens its conversation; a Pi CLI session path is refused.
      const second = yield* openRuntime(adapter, box.workspace, "session-2");
      const resumed = yield* ensureThread(second.runtime, box.workspace, providerThread);
      assert.strictEqual(
        resumed.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      const snapshot = yield* second.runtime.readThreadSnapshot({ providerThread: resumed });
      assert.deepStrictEqual(
        snapshot.messages.map((message) => [message.role, message.text]),
        [
          ["user", "hello durable"],
          ["assistant", "echo: hello durable"],
        ],
      );
      const cliRef = yield* ensureThread(second.runtime, box.workspace, {
        ...providerThread,
        nativeThreadRef: {
          driver: providerThread.driver,
          nativeId: "/home/u/.pi/agent/sessions/x.jsonl",
          strength: "strong",
        },
      }).pipe(Effect.flip);
      assert.strictEqual(cliRef._tag, "ProviderAdapterEnsureThreadError");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("runs the read tool and shows it as a tool item", () =>
    Effect.gen(function* () {
      const box = yield* sandbox;
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(box.file("notes.txt"), "twelve chars");
      const { adapter } = yield* openAdapter(box);
      const { runtime, takeEvent, seen } = yield* openRuntime(adapter, box.workspace);
      const providerThread = yield* ensureThread(runtime, box.workspace);
      yield* startTurn(runtime, providerThread, box.workspace, "tool:read notes.txt");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      const tool = seen.findLast(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
      );
      assert.isTrue(
        tool?.type === "turn_item.updated" &&
          tool.turnItem.type === "dynamic_tool" &&
          tool.turnItem.toolName === "read" &&
          tool.turnItem.status === "completed",
      );
      assert.deepStrictEqual(assistantText(seen), ["read returned 12 characters."]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("Stop aborts the durable run and ends the turn as interrupted", () =>
    Effect.gen(function* () {
      const box = yield* sandbox;
      const { adapter } = yield* openAdapter(box, { tokensPerSecond: 20 });
      const { runtime, takeEvent } = yield* openRuntime(adapter, box.workspace);
      const providerThread = yield* ensureThread(runtime, box.workspace);
      yield* startTurn(runtime, providerThread, box.workspace, "slow: keep talking");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      yield* takeEvent((event) => event.type === "message.updated" && event.message.streaming);
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: running.providerTurn.id,
        requestRuntimeRestart: true,
      });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.live("refuses a workspace-mutating tool profile outside Full access", () =>
    Effect.gen(function* () {
      const box = yield* sandbox;
      const { adapter } = yield* openAdapter(box, { tools: "coding" });
      const { runtime } = yield* openRuntime(adapter, box.workspace);
      const providerThread = yield* ensureThread(runtime, box.workspace);
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-durable");
      const runId = RunId.make(`run:${threadId}:1`);
      const error = yield* runtime
        .startTurn({
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: "project:fixture:durable" as OrchestrationV2AppThread["projectId"],
            title: "Durable test thread",
            providerInstanceId: INSTANCE_ID,
            modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
          rootNodeId: NodeId.make(`node:${runId}:root`),
          providerThread,
          message: {
            messageId: `message:${threadId}:1` as never,
            text: "write something",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: { instanceId: INSTANCE_ID, model: FIXTURE_MODEL },
          runtimePolicy: ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "approval-required",
            interactionMode: "default",
            cwd: box.workspace,
          }),
        })
        .pipe(Effect.flip);
      assert.strictEqual(error._tag, "ProviderAdapterTurnStartError");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
