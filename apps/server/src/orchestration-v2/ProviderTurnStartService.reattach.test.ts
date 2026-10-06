/**
 * The W02 reattach entry with mocked stores and session: what it does when
 * there is nothing to adopt, when it loses the run, and when a resume fails.
 */
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuthService from "../provider/Services/ProviderAuthService.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as DurableReattach from "./DurableReattach.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { ProviderAdapterResumeThreadError } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnStart from "./ProviderTurnStartService.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";

const STORE_ID = "00000000-0000-4000-8000-00000000000a";
const threadId = ThreadId.make("thread_reattach");
const runId = RunId.make("run_reattach");
const laterRunId = RunId.make("run_reattach_later");
const attemptId = RunAttemptId.make("attempt_reattach");
const rootNodeId = NodeId.make("node_reattach_root");
const answerNodeId = NodeId.make("node_reattach_answer");
const providerThreadId = ProviderThreadId.make("provider_thread_reattach");
const providerSessionId = ProviderSessionId.make("provider_session_reattach");
const instanceId = ProviderInstanceId.make("pi-durable");
const pi = ProviderDriverKind.make("pi");

const projectionWith = (input: {
  readonly runStatus: string;
  readonly laterRunStatus?: string;
}) => {
  const providerThread = {
    id: providerThreadId,
    driver: pi,
    providerInstanceId: instanceId,
    providerSessionId,
    nativeThreadRef: { driver: pi, nativeId: `pi-durable:${STORE_ID}:7`, strength: "strong" },
    status: "active",
  };
  return {
    thread: { id: threadId, projectId: ProjectId.make("project_reattach") },
    runs: [
      {
        id: runId,
        status: input.runStatus,
        rootNodeId,
        activeAttemptId: attemptId,
        providerThreadId,
        providerInstanceId: instanceId,
        userMessageId: MessageId.make("message_reattach"),
        ordinal: 1,
        modelSelection: { instanceId, model: "faux/faux-1" },
      },
      ...(input.laterRunStatus === undefined
        ? []
        : [{ id: laterRunId, status: input.laterRunStatus, ordinal: 2 }]),
    ],
    nodes: [
      {
        id: rootNodeId,
        runId,
        status: "running",
        checkpointScopeId: CheckpointScopeId.make("scope_reattach"),
      },
      { id: answerNodeId, runId, status: "running" },
    ],
    attempts: [{ id: attemptId, runId, status: "running" }],
    providerThreads: [providerThread],
    providerTurns: [
      {
        id: ProviderTurnId.make("provider_turn_reattach"),
        runAttemptId: attemptId,
        status: "running",
        ordinal: 1,
      },
    ],
    providerSessions: [],
    messages: [
      { id: MessageId.make("message_reattach"), text: "hello", attachments: [] },
      {
        id: MessageId.make("message_answer"),
        runId,
        nodeId: answerNodeId,
        streaming: true,
      },
    ],
    checkpointScopes: [{ id: CheckpointScopeId.make("scope_reattach") }],
    turnItems: [
      {
        id: TurnItemId.make("turn_item_answer"),
        runId,
        nodeId: answerNodeId,
        type: "assistant_message",
        status: "running",
        ordinal: 101,
      },
    ],
    contextHandoffs: [],
    contextTransfers: [],
  } as unknown as OrchestrationV2ThreadProjection;
};

const harness = (input: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly attachCommits?: boolean;
  readonly resumeFails?: boolean;
}) => {
  const abandoned: Array<DurableReattach.DurableConversationRef> = [];
  const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
  let started = 0;
  const providerThread = input.projection.providerThreads[0]!;
  const session = {
    driver: pi,
    providerSession: { id: providerSessionId, status: "ready" },
    resumeThread: () =>
      input.resumeFails === true
        ? Effect.fail(
            new ProviderAdapterResumeThreadError({
              driver: pi,
              providerSessionId,
              providerThreadId,
              cause: "conversation 7 does not exist",
            }),
          )
        : Effect.succeed(providerThread),
  };
  const layer = ProviderTurnStart.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
        Layer.mock(EventSink.EventSinkV2)({
          writeIfRunCurrent: (write) => {
            writes.push(write.events);
            return Effect.succeed({
              committed: input.attachCommits ?? true,
              storedEvents: [],
            } as never);
          },
        }),
        IdAllocator.layer,
        Layer.succeed(FileSystem.FileSystem, { exists: () => Effect.succeed(true) } as never),
        Layer.mock(GitWorkflow.GitWorkflowService)({}),
        Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeed(Option.none()) }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getTurnStartContext: () =>
            Effect.succeed({ ...input.projection, hasConversation: true } as never),
          getRuntimeRecoveryProjection: () => Effect.succeed(input.projection),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          open: () => Effect.succeed(session as never),
        }),
        Layer.mock(ProviderAuthService.ProviderAuthService)({}),
        Layer.mock(RunExecutionService.RunExecutionServiceV2)({
          startRootRun: () =>
            Effect.sync(() => {
              started += 1;
            }),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: () =>
            Effect.succeed({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: "/tmp",
            } as never),
        }),
        Layer.succeed(
          DurableReattach.DurableReattachPlan,
          DurableReattach.DurableReattachPlan.of({
            complete: () => Effect.void,
            conversationsFor: () => Effect.succeed([]),
            registerAbandon: () => Effect.void,
            abandon: (_instanceId, conversation) =>
              Effect.sync(() => {
                abandoned.push(conversation);
              }),
          }),
        ),
      ),
    ),
  );
  const reattach = ProviderTurnStart.ProviderTurnStartServiceV2.pipe(
    Effect.flatMap((service) => service.reattach({ threadId, runId })),
    Effect.provide(layer),
  );
  return { reattach, abandoned, writes, started: () => started };
};

it.effect("a run that ended before its reattach stops the work kept for it", () =>
  Effect.gen(function* () {
    const test = harness({ projection: projectionWith({ runStatus: "cancelled" }) });
    yield* test.reattach;
    assert.deepEqual(test.abandoned, [{ storeId: STORE_ID, conversationId: 7 }]);
    assert.strictEqual(test.started(), 0);
  }),
);

it.effect("a newer run on the thread keeps the conversation running", () =>
  Effect.gen(function* () {
    const test = harness({
      projection: projectionWith({ runStatus: "interrupted", laterRunStatus: "running" }),
    });
    yield* test.reattach;
    assert.deepEqual(test.abandoned, []);
  }),
);

it.effect("a reattach that loses the run while attaching stops the kept work", () =>
  Effect.gen(function* () {
    const test = harness({
      projection: projectionWith({ runStatus: "running" }),
      attachCommits: false,
    });
    yield* test.reattach;
    assert.deepEqual(test.abandoned, [{ storeId: STORE_ID, conversationId: 7 }]);
    assert.strictEqual(test.started(), 0, "nothing adopts the run");
  }),
);

it.effect("a failed resume fails the run and settles its open output", () =>
  Effect.gen(function* () {
    const test = harness({
      projection: projectionWith({ runStatus: "running" }),
      resumeFails: true,
    });
    yield* test.reattach;
    const events = test.writes.flat();
    const statusOf = (type: string, id: string) =>
      events.flatMap((event) =>
        event.type === type && "payload" in event && (event.payload as { id: string }).id === id
          ? [(event.payload as { status?: string; streaming?: boolean }).status ?? "streaming"]
          : [],
      );
    assert.deepEqual(statusOf("run.updated", runId), ["failed"]);
    assert.deepEqual(statusOf("turn-item.updated", "turn_item_answer"), ["cancelled"]);
    assert.deepEqual(statusOf("node.updated", answerNodeId), ["cancelled"]);
    assert.isTrue(
      events.some(
        (event) =>
          event.type === "message.updated" &&
          event.payload.id === "message_answer" &&
          !event.payload.streaming,
      ),
    );
    assert.strictEqual(test.started(), 0);
  }),
);
