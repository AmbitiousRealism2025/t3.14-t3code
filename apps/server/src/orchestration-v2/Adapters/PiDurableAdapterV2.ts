/**
 * PiDurableAdapterV2 — T3.14 adapter that runs Pi on the box-side Pi-Durable
 * worker (`t3-14 worker`) instead of `pi --mode rpc`. It presents as driver
 * `pi`, so unchanged clients keep Pi's settings form, icon and model defaults.
 *
 * One worker per adapter instance owns the durable store. It is spawned on
 * first use, speaks Pi's RPC framing (so `PiRpc` is the transport) and lives
 * until the instance scope closes. A crashed worker is respawned on the next
 * session; a worker still releasing its owner lease (instance recreated on a
 * settings change) is waited for.
 *
 * Each provider session binds one durable conversation. The native thread
 * ref is `pi-durable:<storeId>:<conversationId>`; a Pi CLI session path is
 * never resumed as a durable conversation, and this ref is never a path.
 *
 * Turn lifecycle: a T3 turn is the set of submissions it admitted (the start
 * input plus any steering). It terminalizes once every one of them settles:
 * `done` completes, `unanswered/aborted` is interrupted, anything else fails.
 * Submissions use request IDs derived from T3 message IDs, so a retried start
 * returns the original durable submission.
 *
 * S01 scope: no approvals, rollback, fork, compaction or retry items yet.
 * Stop aborts the conversation's durable work; it never restarts the worker.
 */
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import {
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type PiDurableSettings,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import {
  makePiRpcConnection,
  parsePiModelSlug,
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
  PiRpcError,
  type PiRpcRecord,
} from "./PiRpc.ts";
import { PI_PROVIDER } from "./PiAdapterV2.ts";

/** Durable worker wire protocol version this adapter speaks. */
// 2: the worker boots holding recovered work until `owner.reconcile` (D03).
const DURABLE_WORKER_PROTOCOL = 2;
const WORKER_READY_TIMEOUT = Duration.seconds(30);
/** How long a new worker waits for a previous owner to release the store. */
const OWNER_HANDOVER_WINDOW = Duration.seconds(15);
const WORKER_REQUEST_TIMEOUT_MS = 15_000;
const STREAM_FLUSH_MS = 50;
/** Pi's "inherit the configured default" model slug; the worker says what it resolves to. */
export const PI_INHERIT_MODEL_SLUG = "default";

/** How to leave inspection: the release needs the owner lease, which the running worker holds. */
export const DURABLE_INSPECTION_RELEASE_HINT =
  "Stop T3.14, run `t3-14 inspect release --note <reason>` on the box, then start it again.";

const DURABLE_REF_PATTERN = /^pi-durable:([0-9a-f-]+):(\d+)$/;

/**
 * Whether a durable adapter failure came from a worker request that got no
 * answer (timeout, worker gone), so the same call may still succeed. Answers
 * the worker gave, and the adapter's own refusals, are final.
 */
export function isTransientDurableFailure(error: unknown): boolean {
  for (let cause: unknown = error; typeof cause === "object" && cause !== null;) {
    const record = cause as {
      readonly _tag?: unknown;
      readonly payload?: unknown;
      readonly cause?: unknown;
    };
    if (record._tag === "ProviderAdapterProtocolError") {
      return (record.payload as { readonly transient?: unknown } | undefined)?.transient === true;
    }
    cause = record.cause;
  }
  return false;
}

export function parseDurableThreadRef(
  ref: string,
): { readonly storeId: string; readonly conversationId: number } | undefined {
  const match = DURABLE_REF_PATTERN.exec(ref);
  if (match === null) return undefined;
  return { storeId: match[1]!, conversationId: Number(match[2]) };
}

export const PiDurableProviderCapabilitiesV2 = {
  // Tool policy is enforced by the worker's tool profile, not by T3 modes yet.
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "none",
  },
} satisfies OrchestrationV2ProviderCapabilities;

// ── worker ────────────────────────────────────────────────────

export class PiDurableWorkerError extends Schema.TaggedError<PiDurableWorkerError>()(
  "PiDurableWorkerError",
  {
    detail: Schema.String,
    /** The worker's own error name, e.g. `OwnerConflictError`. */
    errorName: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi-Durable worker: ${this.detail}`;
  }
}

export interface DurableWorkerModel {
  readonly provider: string;
  readonly modelId: string;
  readonly name: string;
  readonly contextWindow: number;
  readonly reasoning: boolean;
  /** pi-ai's per-level overrides; with `reasoning` it decides which thinking levels exist. */
  readonly thinkingLevelMap?: Readonly<Record<string, string | null>>;
}

export interface DurableWorkerStatus {
  readonly storeId: string;
  readonly ownerEpoch: number;
  readonly scheduling: string;
  readonly reasons: ReadonlyArray<string>;
}

export interface DurableWorker {
  /** Status from the worker's `ready` frame. */
  readonly status: DurableWorkerStatus;
  /** Status read from the worker now. */
  readonly currentStatus: Effect.Effect<DurableWorkerStatus, PiDurableWorkerError>;
  /** What a "default" model selection resolves to; null when the worker has no model. */
  readonly defaultModel: { readonly provider: string; readonly modelId: string } | null;
  /** Tool profile the worker offers: `none`, `read` or `coding` (mutating). */
  readonly tools: string;
  readonly models: ReadonlyArray<DurableWorkerModel>;
  readonly request: (
    method: string,
    params?: PiRpcRecord,
    timeoutMs?: number,
  ) => Effect.Effect<unknown, PiDurableWorkerError>;
  /** Agent event batches for one conversation, until the scope closes or the worker dies. */
  readonly subscribe: (
    conversationId: number,
  ) => Effect.Effect<Queue.Dequeue<PiRpcRecord, PiDurableWorkerError>, never, Scope.Scope>;
  readonly isAlive: () => boolean;
}

const isWorkerError = Schema.is(PiDurableWorkerError);
const isPiRpcError = Schema.is(PiRpcError);

function workerFailure(cause: unknown): PiDurableWorkerError {
  if (isWorkerError(cause)) return cause;
  if (isPiRpcError(cause)) {
    // A failed response's detail is the worker's JSON error; keep its name.
    const name = /"name":"([^"]+)"/.exec(cause.detail ?? "")?.[1];
    return new PiDurableWorkerError({
      detail: cause.detail ?? cause.message,
      ...(name === undefined ? {} : { errorName: name }),
      cause,
    });
  }
  return new PiDurableWorkerError({ detail: String(cause), cause });
}

function parseWorkerStatus(record: unknown): DurableWorkerStatus {
  return {
    storeId: recordString(record, "storeId") ?? "",
    ownerEpoch: recordNumber(record, "ownerEpoch") ?? 0,
    scheduling: recordString(record, "scheduling") ?? "inhibited",
    reasons: (recordField(record, "reasons") as ReadonlyArray<string> | undefined) ?? [],
  };
}

/** A conversation whose recovered work T3 reattaches instead of cancelling (W02). */
export interface DurableKeptConversation {
  readonly storeId: string;
  readonly conversationId: number;
}

const makeDurableWorker = Effect.fnUntraced(function* (input: {
  readonly launch: PiDurableSettings;
  readonly env: NodeJS.ProcessEnv;
  readonly keep: ReadonlyArray<DurableKeptConversation>;
}) {
  const scope = yield* Effect.scope;
  const connection = yield* makePiRpcConnection({
    command: input.launch.command,
    args: input.launch.args,
    cwd: undefined,
    env: input.env,
  }).pipe(Effect.mapError(workerFailure));

  const ready = yield* Effect.gen(function* () {
    while (true) {
      const record = yield* Queue.take(connection.events);
      if (record["type"] === "ready") return record;
      if (record["type"] === "fatal") {
        const error = recordField(record, "error");
        const errorName = recordString(error, "name");
        return yield* new PiDurableWorkerError({
          detail: recordString(error, "message") ?? "worker failed to start",
          ...(errorName === undefined ? {} : { errorName }),
        });
      }
    }
  }).pipe(
    Effect.mapError(workerFailure),
    Effect.timeoutOrElse({
      duration: WORKER_READY_TIMEOUT,
      orElse: () =>
        Effect.fail(new PiDurableWorkerError({ detail: "worker did not report ready in time" })),
    }),
  );
  if (recordNumber(ready, "protocol") !== DURABLE_WORKER_PROTOCOL) {
    return yield* new PiDurableWorkerError({
      detail: `worker speaks protocol ${String(ready["protocol"])}, adapter speaks ${DURABLE_WORKER_PROTOCOL}`,
    });
  }
  const booted = parseWorkerStatus(recordField(ready, "status"));
  const defaultRecord = recordField(ready, "defaultModel");
  const defaultProvider = recordString(defaultRecord, "provider");
  const defaultModelId = recordString(defaultRecord, "modelId");
  const models = ((recordField(ready, "models") as ReadonlyArray<DurableWorkerModel> | undefined) ??
    []) as ReadonlyArray<DurableWorkerModel>;

  const subscribers = new Map<number, Set<Queue.Queue<PiRpcRecord, PiDurableWorkerError>>>();
  let alive = true;

  yield* Effect.gen(function* () {
    while (true) {
      const record = yield* Queue.take(connection.events);
      const conversationId = recordNumber(record, "conversationId");
      if (
        (record["type"] === "events" || record["type"] === "watch-ended") &&
        conversationId !== undefined
      ) {
        for (const queue of subscribers.get(conversationId) ?? []) {
          yield* Queue.offer(queue, record);
        }
      } else if (record["type"] === "report") {
        yield* Effect.logWarning("Pi-Durable worker report", {
          message: recordString(record, "message"),
        });
      }
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        alive = false;
        const error = new PiDurableWorkerError({
          detail: "worker exited",
          cause: Cause.squash(cause),
        });
        for (const queues of subscribers.values()) {
          for (const queue of queues) yield* Queue.fail(queue, error);
        }
      }),
    ),
    Effect.forkIn(scope),
  );

  const request: DurableWorker["request"] = (
    method,
    params = {},
    timeoutMs = WORKER_REQUEST_TIMEOUT_MS,
  ) =>
    connection.request({ ...params, type: method }, timeoutMs).pipe(Effect.mapError(workerFailure));
  // A worker boots holding everything it recovered (D03). T3 reattaches the
  // runs startup recovery left running (W02); everything else T3 cancelled
  // is aborted before any of it can run unseen.
  const keep = input.keep.flatMap((kept) =>
    kept.storeId === booted.storeId ? [kept.conversationId] : [],
  );
  const status =
    booted.reasons.length === 1 && booted.reasons[0] === "reconciliation-pending"
      ? yield* request(
          "owner.reconcile",
          keep.length === 0
            ? { policy: "abort-recovered" }
            : { policy: "keep-conversations", conversations: keep },
        ).pipe(
          Effect.tap((result) =>
            Effect.logInfo("Pi-Durable worker reconciled recovered work", {
              kept: keep,
              aborted: recordField(result, "aborted"),
            }),
          ),
          Effect.map((result) => parseWorkerStatus(recordField(result, "status"))),
        )
      : booted;
  return {
    status,
    currentStatus: request("owner.status").pipe(Effect.map(parseWorkerStatus)),
    defaultModel:
      defaultProvider === undefined || defaultModelId === undefined
        ? null
        : { provider: defaultProvider, modelId: defaultModelId },
    tools: recordString(ready, "tools") ?? "none",
    models,
    request,
    subscribe: (conversationId) =>
      Effect.acquireRelease(
        Effect.gen(function* () {
          const queue = yield* Queue.unbounded<PiRpcRecord, PiDurableWorkerError>();
          const set = subscribers.get(conversationId) ?? new Set();
          set.add(queue);
          subscribers.set(conversationId, set);
          return queue;
        }),
        (queue) =>
          Effect.sync(() => {
            subscribers.get(conversationId)?.delete(queue);
          }),
      ),
    isAlive: () => alive,
  } satisfies DurableWorker;
});

/**
 * One worker per adapter instance, created on first use and respawned after
 * it dies. Owner conflicts are retried for a short handover window.
 *
 * `keepConversations` resolves once startup recovery has decided which
 * durable runs to reattach; the first worker waits for it and keeps their
 * conversations' work. A respawned worker keeps nothing: the runs it would
 * have served failed with the worker that died.
 */
export const makeDurableWorkerManager = Effect.fnUntraced(function* (input: {
  readonly launch: PiDurableSettings;
  readonly env: NodeJS.ProcessEnv;
  readonly keepConversations?: Effect.Effect<ReadonlyArray<DurableKeptConversation>>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const permit = yield* Semaphore.make(1);
  let keepConversations = input.keepConversations;
  let current: { readonly worker: DurableWorker; readonly scope: Scope.Closeable } | null = null;
  // Stopping a worker waits out PiRpc's termination grace in wall-clock time,
  // including under a test clock, so the instance closes it on the live clock.
  const liveClock = Effect.provideService(Clock.Clock, Clock.Clock.defaultValue());
  yield* Effect.addFinalizer(() =>
    permit
      .withPermits(1)(
        Effect.suspend(() => {
          const closing = current;
          current = null;
          return closing === null ? Effect.void : Scope.close(closing.scope, Exit.void);
        }),
      )
      .pipe(liveClock),
  );

  const get: Effect.Effect<DurableWorker, PiDurableWorkerError> = permit.withPermits(1)(
    Effect.gen(function* () {
      if (current !== null && current.worker.isAlive()) return current.worker;
      if (current !== null) {
        yield* Scope.close(current.scope, Exit.void).pipe(liveClock);
        current = null;
      }
      const keep = keepConversations === undefined ? [] : yield* keepConversations;
      const attempt = Effect.gen(function* () {
        const workerScope = yield* Scope.make("sequential");
        return yield* makeDurableWorker({ launch: input.launch, env: input.env, keep }).pipe(
          Scope.provide(workerScope),
          Effect.map((worker) => ({ worker, scope: workerScope })),
          Effect.tapError(() => Scope.close(workerScope, Exit.void).pipe(liveClock)),
        );
      });
      const started = yield* attempt.pipe(
        Effect.retry({
          while: (error) => error.errorName === "OwnerConflictError",
          schedule: Schedule.spaced("500 millis").pipe(
            Schedule.upTo({ duration: OWNER_HANDOVER_WINDOW }),
          ),
        }),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      current = started;
      keepConversations = undefined;
      yield* Effect.logInfo("Pi-Durable worker ready", {
        storeId: started.worker.status.storeId,
        ownerEpoch: started.worker.status.ownerEpoch,
        scheduling: started.worker.status.scheduling,
      });
      return started.worker;
    }),
  );
  return { get };
});

export type DurableWorkerManager = Effect.Success<ReturnType<typeof makeDurableWorkerManager>>;

// ── adapter ───────────────────────────────────────────────────

export interface PiDurableAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly workers: DurableWorkerManager;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig.ServerConfig["Service"];
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: PI_PROVIDER, nativeId, strength };
}

/** Concatenated text of a pi-ai content array (text blocks only). */
function contentText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.map((block) => (block?.type === "text" ? String(block.text ?? "") : "")).join("");
}

function blockText(
  block: unknown,
): { kind: "assistant_message" | "reasoning"; text: string } | null {
  const type = recordString(block, "type");
  if (type === "text")
    return { kind: "assistant_message", text: recordString(block, "text") ?? "" };
  if (type === "thinking")
    return { kind: "reasoning", text: recordString(block, "thinking") ?? "" };
  return null;
}

interface StreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  /** The first update goes out at once; later ones are throttled. */
  emitted: boolean;
  readonly startedAt: DateTime.Utc;
}

interface ToolState {
  readonly nativeItemId: string;
  readonly toolName: string;
  readonly args: unknown;
  output: string;
  readonly startedAt: DateTime.Utc;
}

interface ActiveDurableTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  messageOrdinal: number;
  readonly streamItems: Map<string, StreamItemState>;
  readonly tools: Map<string, ToolState>;
  /** Durable submission IDs this turn admitted; settled ones carry their record. */
  readonly submissions: Map<number, PiRpcRecord | null>;
  /** Submit calls still in flight; the turn cannot end before they return. */
  pendingAdmissions: number;
  interrupted: boolean;
  failure: ReturnType<typeof makeProviderFailure> | null;
  lastLiveUsedTokens: number | null;
}

export function makePiDurableAdapterV2(
  options: PiDurableAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: PI_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: PI_PROVIDER,
    getCapabilities: () => Effect.succeed(PiDurableProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("PiDurableAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? options.serverConfig.cwd;
      const worker = yield* options.workers.get.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: PI_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      const call = (method: string, params?: PiRpcRecord, retryUnanswered = false) =>
        worker.request(method, params).pipe(
          // An unanswered request (timeout, no worker reply) may still have
          // landed. Requests whose replay is idempotent are asked again so
          // the original outcome is recovered instead of dropped.
          Effect.catchIf(
            (cause) => retryUnanswered && cause.errorName === undefined && worker.isAlive(),
            () => worker.request(method, params),
          ),
          Effect.mapError((cause) =>
            protocolError(
              cause.errorName === "SchedulingInhibited"
                ? `the durable runtime is not scheduling work (${cause.detail}). ${DURABLE_INSPECTION_RELEASE_HINT}`
                : `worker ${method} failed: ${cause.detail}`,
              cause.errorName === undefined ? { transient: true } : { errorName: cause.errorName },
            ),
          ),
        );

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: PI_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: PiDurableProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const permit = yield* Semaphore.make(1);
      let providerThread: OrchestrationV2ProviderThread | null = null;
      let conversationId: number | null = null;
      let watchScope: Scope.Closeable | null = null;
      let activeTurn: ActiveDurableTurn | null = null;
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      /** Settlement records that arrived before their submit call returned. */
      const earlySettlements = new Map<number, PiRpcRecord>();
      /**
       * Submissions already in the conversation when this session bound it:
       * work resumed from before a restart that no T3 turn owns. A run made of
       * them only is not attributed to the active turn. Deciding what to do
       * with such work is the D03 reconcile; until then its output stays out
       * of the next turn.
       */
      const foreignSubmissions = new Set<number>();
      let runIsForeign = false;
      /** Inputs of the run executing now, from the watch snapshot or `run_start`. */
      let currentRunInputs: ReadonlyArray<number> = [];
      const recomputeRunIsForeign = () => {
        runIsForeign =
          currentRunInputs.length > 0 &&
          currentRunInputs.every((submissionId) => foreignSubmissions.has(submissionId));
      };
      /**
       * The watch snapshot and the output of foreign runs since it was taken.
       * A reattached turn (W02) adopts its run from both: the snapshot holds
       * what the run wrote before this session watched, the backlog the rest.
       */
      let boundSnapshot: unknown = undefined;
      /** Submissions live in the conversation when the watch began: a reattached turn's own. */
      let liveAtBind: ReadonlySet<number> = new Set();
      let foreignBacklog: Array<{
        readonly runInputs: ReadonlyArray<number>;
        readonly event: PiRpcRecord;
      }> = [];

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) =>
        Effect.gen(function* () {
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt: yield* DateTime.now };
          yield* emit({
            type: "provider_session.updated",
            driver: PI_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (patch: Partial<OrchestrationV2ProviderThread>) =>
        Effect.gen(function* () {
          if (providerThread === null) return;
          providerThread = { ...providerThread, ...patch, updatedAt: yield* DateTime.now };
          yield* emit({ type: "provider_thread.updated", driver: PI_PROVIDER, providerThread });
        });

      /** The concrete `provider/model` slug a selection runs on; "default" asks the worker. */
      const resolveSlug = (slug: string): string | null =>
        slug !== PI_INHERIT_MODEL_SLUG
          ? slug
          : worker.defaultModel === null
            ? null
            : `${worker.defaultModel.provider}/${worker.defaultModel.modelId}`;

      const contextWindowFor = (slug: string | null): number | null => {
        const parsed = slug === null ? null : parsePiModelSlug(slug);
        const model = worker.models.find(
          (candidate) =>
            parsed !== null &&
            candidate.provider === parsed.provider &&
            candidate.modelId === parsed.modelId,
        );
        return model?.contextWindow ?? null;
      };

      // ── items ──────────────────────────────────────────────

      const itemOrdinal = (turn: ActiveDurableTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const baseItemFields = (
        turn: ActiveDurableTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({ driver: PI_PROVIDER, nativeItemId }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({ driver: PI_PROVIDER, nativeItemId }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveDurableTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: PI_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({ driver: PI_PROVIDER, nativeItemId }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      const emitStreamItem = (turn: ActiveDurableTurn, item: StreamItemState, streaming: boolean) =>
        Effect.gen(function* () {
          const emittedAt = yield* DateTime.now;
          const status = streaming ? "running" : "completed";
          const completedAt = streaming ? null : emittedAt;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            status,
            item.startedAt,
            completedAt,
          );
          if (item.kind === "reasoning") {
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                ...base,
                status,
                title: null,
                completedAt,
                type: "reasoning",
                text: item.text,
                streaming,
              },
            });
            return;
          }
          const messageId = idAllocator.derive.messageFromProviderItem({
            driver: PI_PROVIDER,
            nativeItemId: item.nativeItemId,
          });
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...base,
              status,
              title: null,
              completedAt,
              type: "assistant_message",
              messageId,
              text: item.text,
              streaming,
            },
          });
          yield* emit({
            type: "message.updated",
            driver: PI_PROVIDER,
            message: {
              id: messageId,
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              nodeId: base.nodeId,
              role: "assistant",
              text: item.text,
              attachments: [],
              streaming,
              createdBy: "agent",
              creationSource: "provider",
              createdAt: item.startedAt,
              updatedAt: emittedAt,
            },
          });
        });

      const scheduleStreamFlush = (turn: ActiveDurableTurn, item: StreamItemState) =>
        Effect.gen(function* () {
          if (item.flushScheduled || item.completed) return;
          if (!item.emitted) {
            item.emitted = true;
            return yield* emitStreamItem(turn, item, true);
          }
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              permit.withPermits(1)(
                Effect.suspend(() => {
                  item.flushScheduled = false;
                  return item.completed ? Effect.void : emitStreamItem(turn, item, true);
                }),
              ),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItem = Effect.fnUntraced(function* (
        turn: ActiveDurableTurn,
        kind: StreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = `${turn.providerTurn.id}:m${turn.messageOrdinal}:c${contentIndex}`;
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const item: StreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          emitted: false,
          startedAt: yield* DateTime.now,
        };
        turn.streamItems.set(nativeItemId, item);
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const setBlockText = Effect.fnUntraced(function* (
        turn: ActiveDurableTurn,
        contentIndex: number,
        block: unknown,
      ) {
        const parsed = blockText(block);
        if (parsed === null) return;
        const item = yield* streamItem(turn, parsed.kind, contentIndex);
        item.text = parsed.text;
        yield* scheduleStreamFlush(turn, item);
      });

      const completeStreamItems = (turn: ActiveDurableTurn) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) =>
            Effect.suspend(() => {
              item.completed = true;
              return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
            }),
          { discard: true },
        );

      const emitTool = Effect.fnUntraced(function* (
        turn: ActiveDurableTurn,
        tool: ToolState,
        phase: "running" | "completed" | "failed" | "interrupted",
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = phase === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          tool.nativeItemId,
          "tool_call",
          phase,
          tool.startedAt,
          completedAt,
        );
        const shared = {
          ...baseItemFields(turn, tool.nativeItemId, tool.startedAt, emittedAt),
          status: phase,
          completedAt,
          title: tool.toolName,
        } as const;
        const output = tool.output.length > 0 ? { output: tool.output } : {};
        if (tool.toolName === "bash") {
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...shared,
              type: "command_execution",
              input: recordString(tool.args, "command") ?? "",
              ...output,
            },
          });
          return;
        }
        const fileName = recordString(tool.args, "path");
        if ((tool.toolName === "edit" || tool.toolName === "write") && fileName !== undefined) {
          const newStr = tool.toolName === "write" ? recordString(tool.args, "content") : undefined;
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...shared,
              type: "file_change",
              fileName,
              ...(newStr === undefined ? {} : { newStr }),
            },
          });
          return;
        }
        yield* emit({
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: {
            ...shared,
            type: "dynamic_tool",
            toolName: tool.toolName,
            input: tool.args ?? {},
            ...output,
          },
        });
      });

      const reportLiveUsage = (turn: ActiveDurableTurn, usage: unknown) =>
        Effect.gen(function* () {
          const usedTokens = recordNumber(usage, "totalTokens");
          const maxTokens = contextWindowFor(appliedModel);
          if (
            usedTokens === undefined ||
            usedTokens <= 0 ||
            usedTokens === turn.lastLiveUsedTokens ||
            maxTokens === null
          ) {
            return;
          }
          turn.lastLiveUsedTokens = usedTokens;
          yield* emit({
            type: "provider_turn.updated",
            driver: PI_PROVIDER,
            threadId: turn.turnInput.threadId,
            providerTurn: {
              ...turn.providerTurn,
              tokenUsage: {
                usedTokens,
                maxTokens,
                updatedAt: DateTime.formatIso(yield* DateTime.now),
              },
            },
          });
        });

      // ── turn end ───────────────────────────────────────────

      const finalizeTurn = Effect.fnUntraced(function* (turn: ActiveDurableTurn) {
        if (activeTurn !== turn) return;
        activeTurn = null;
        const completedAt = yield* DateTime.now;
        yield* completeStreamItems(turn);
        for (const tool of turn.tools.values()) {
          yield* emitTool(turn, tool, turn.interrupted ? "interrupted" : "failed");
        }
        turn.tools.clear();
        const records = Array.from(turn.submissions.values());
        const aborted = records.some(
          (record) =>
            recordString(record, "status") === "unanswered" &&
            recordString(record, "reason") === "aborted",
        );
        const interrupted = turn.interrupted || aborted;
        const unanswered = records.find(
          (record) => recordString(record, "status") === "unanswered",
        );
        const failure = interrupted
          ? null
          : (turn.failure ??
            (unanswered === undefined || unanswered === null
              ? null
              : makeProviderFailure({
                  message: `Durable submission was not answered: ${recordString(unanswered, "reason") ?? "unknown"}`,
                })));
        const primary = Array.from(turn.submissions.keys())[0];
        yield* emit({
          type: "provider_turn.updated",
          driver: PI_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            ...(primary === undefined
              ? {}
              : {
                  nativeTurnRef: providerRef(
                    `pi-durable-submission:${worker.status.storeId}:${primary}`,
                  ),
                }),
            status: interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
          },
        });
        yield* updateProviderThread({ status: "idle" });
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, failureItemId, completedAt, completedAt),
              status: "failed",
              title: null,
              completedAt,
              type: "error",
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: PI_PROVIDER,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            threadDisposition: "reusable",
          });
          return;
        }
        yield* emit({
          type: "turn.terminal",
          driver: PI_PROVIDER,
          providerThreadId: turn.turnInput.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          status: interrupted ? "interrupted" : "completed",
          failure: null,
          threadDisposition: "reusable",
        });
      });

      const settleIfDone = (turn: ActiveDurableTurn) =>
        turn.pendingAdmissions === 0 &&
        turn.submissions.size > 0 &&
        Array.from(turn.submissions.values()).every((record) => record !== null)
          ? finalizeTurn(turn)
          : Effect.void;

      // ── event mapping ──────────────────────────────────────

      const handleEvent = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const type = recordString(event, "type");
        if (type === "submission") {
          const record = recordField(event, "record") as PiRpcRecord | undefined;
          const id = recordNumber(record, "id");
          const status = recordString(record, "status");
          if (record === undefined || id === undefined) return;
          if (status !== "done" && status !== "unanswered") return;
          if (foreignSubmissions.delete(id)) {
            // Kept in case a rebind marked one of our in-flight admissions
            // foreign; `admitReserved` claims it once the ID comes back.
            earlySettlements.set(id, record);
            return;
          }
          const turn = activeTurn;
          if (turn !== null && turn.submissions.has(id)) {
            turn.submissions.set(id, record);
            yield* settleIfDone(turn);
          } else {
            earlySettlements.set(id, record);
          }
          return;
        }
        if (type === "run_start") {
          const inputs = recordField(event, "inputs");
          currentRunInputs = Array.isArray(inputs)
            ? inputs.filter((input): input is number => typeof input === "number")
            : [];
          recomputeRunIsForeign();
          return;
        }
        if (type === "run_end") {
          currentRunInputs = [];
          runIsForeign = false;
          return;
        }
        if (runIsForeign) {
          foreignBacklog.push({ runInputs: currentRunInputs, event });
          return;
        }
        const turn = activeTurn;
        if (turn === null) return;
        yield* handleOutput(turn, event);
      });

      /** Maps one output event of the run that `turn` owns. */
      const handleOutput = Effect.fnUntraced(function* (
        turn: ActiveDurableTurn,
        event: PiRpcRecord,
      ) {
        const type = recordString(event, "type");
        switch (type) {
          case "message_start": {
            const message = recordField(event, "message");
            if (recordString(message, "role") !== "assistant") return;
            turn.messageOrdinal += 1;
            const content = recordField(message, "content");
            if (Array.isArray(content)) {
              for (const [index, block] of content.entries())
                yield* setBlockText(turn, index, block);
            }
            return;
          }
          case "message_update": {
            const changes = recordField(event, "changes");
            for (const change of Array.isArray(changes) ? changes : []) {
              const changeType = recordString(change, "type");
              const index = recordNumber(change, "contentIndex") ?? 0;
              if (
                changeType === "text_start" ||
                changeType === "thinking_start" ||
                changeType === "block"
              ) {
                yield* setBlockText(turn, index, recordField(change, "block"));
              } else if (changeType === "text_delta" || changeType === "thinking_delta") {
                const kind = changeType === "text_delta" ? "assistant_message" : "reasoning";
                const item = yield* streamItem(turn, kind, index);
                item.text += recordString(change, "delta") ?? "";
                yield* scheduleStreamFlush(turn, item);
              } else if (changeType === "message") {
                const content = recordField(recordField(change, "message"), "content");
                if (Array.isArray(content)) {
                  for (const [blockIndex, block] of content.entries()) {
                    yield* setBlockText(turn, blockIndex, block);
                  }
                }
              }
            }
            yield* reportLiveUsage(turn, recordField(event, "usage"));
            return;
          }
          case "message_end": {
            const message = (
              recordField(recordField(event, "entry"), "model") as unknown[] | undefined
            )?.[0];
            if (recordString(message, "role") !== "assistant") return;
            const content = recordField(message, "content");
            if (Array.isArray(content)) {
              for (const [index, block] of content.entries()) {
                const parsed = blockText(block);
                if (parsed === null) continue;
                const item = yield* streamItem(turn, parsed.kind, index);
                item.text = parsed.text;
              }
            }
            yield* completeStreamItems(turn);
            if (recordString(message, "stopReason") === "error") {
              turn.failure = makeProviderFailure({
                message: recordString(message, "errorMessage") ?? "The model returned an error.",
              });
            }
            return;
          }
          case "tool_execution_start": {
            const callId = recordString(event, "toolCallId");
            if (callId === undefined) return;
            const tool: ToolState = {
              nativeItemId: `${turn.providerTurn.id}:tool:${callId}`,
              toolName: recordString(event, "toolName") ?? "tool",
              args: recordField(event, "args"),
              output: "",
              startedAt: yield* DateTime.now,
            };
            turn.tools.set(callId, tool);
            yield* emitTool(turn, tool, "running");
            return;
          }
          case "tool_execution_update": {
            const tool = turn.tools.get(recordString(event, "toolCallId") ?? "");
            const output = recordField(event, "output");
            if (tool === undefined || output === undefined) return;
            const set = recordString(output, "set");
            if (set !== undefined) tool.output = set;
            else {
              tool.output = tool.output.slice(recordNumber(output, "trimStart") ?? 0);
              tool.output += recordString(output, "append") ?? "";
            }
            yield* emitTool(turn, tool, "running");
            return;
          }
          case "tool_execution_end": {
            const callId = recordString(event, "toolCallId") ?? "";
            const tool = turn.tools.get(callId);
            if (tool === undefined) return;
            turn.tools.delete(callId);
            const result = (
              recordField(recordField(event, "entry"), "model") as unknown[] | undefined
            )?.[0];
            const text = contentText(recordField(result, "content"));
            if (text.length > 0) tool.output = text;
            const isError = recordField(result, "isError") === true || result === undefined;
            yield* emitTool(
              turn,
              tool,
              isError ? (turn.interrupted ? "interrupted" : "failed") : "completed",
            );
            return;
          }
          case "task_failed": {
            if (recordString(event, "kind") === "pi.generation") {
              turn.failure = makeProviderFailure({
                message: recordString(event, "message") ?? "The durable generation task failed.",
              });
            }
            return;
          }
          default:
            return;
        }
      });

      const pumpEvents = (
        queue: Queue.Dequeue<
          PiRpcRecord,
          ProviderAdapter.ProviderAdapterV2Error | PiDurableWorkerError
        >,
      ) =>
        Effect.gen(function* () {
          while (true) {
            const record = yield* Queue.take(queue);
            if (record["type"] === "watch-ended") {
              return yield* new PiDurableWorkerError({
                detail: `event watch ended: ${recordString(record, "reason") ?? "unknown"}`,
              });
            }
            const batch = recordField(record, "events");
            yield* permit.withPermits(1)(
              Effect.forEach(
                Array.isArray(batch) ? batch : [],
                (event) => handleEvent(event as PiRpcRecord),
                {
                  discard: true,
                },
              ),
            );
          }
        }).pipe(
          Effect.catchCause((cause) =>
            // Closing the watch scope on a rebind interrupts this pump on
            // purpose; only a failure means the worker or watch is gone.
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : permit.withPermits(1)(
                  Effect.gen(function* () {
                    const turn = activeTurn;
                    if (turn !== null) {
                      turn.failure = makeProviderFailure({
                        cause,
                        message: "The Pi-Durable worker stopped.",
                        class: "transport_error",
                      });
                      yield* finalizeTurn(turn);
                    }
                    yield* updateProviderSession("error", "The Pi-Durable worker stopped.");
                    yield* Queue.fail(
                      events,
                      new ProviderAdapter.ProviderAdapterEventStreamError({
                        driver: PI_PROVIDER,
                        providerSessionId: input.providerSessionId,
                        cause: Cause.squash(cause),
                      }),
                    );
                  }),
                ),
          ),
        );

      // ── threads ────────────────────────────────────────────

      const bindConversation = Effect.fnUntraced(function* (id: number) {
        // Rebinding the conversation already watched keeps that watch: closing
        // and reopening it would leave a gap in which a settlement is missed.
        if (watchScope !== null && conversationId === id) return;
        if (watchScope !== null) {
          yield* Scope.close(watchScope, Exit.void);
          watchScope = null;
        }
        const nextScope = yield* Scope.fork(scope);
        const queue = yield* worker.subscribe(id).pipe(Scope.provide(nextScope));
        const watched = yield* call("conversation.watch", { conversationId: id });
        // Mark resumed work before the pump sees its first event.
        const snapshot = recordField(watched, "snapshot");
        const run = recordField(snapshot, "run");
        const runInputs = recordField(run, "inputs");
        const inbox = recordField(snapshot, "inbox");
        foreignSubmissions.clear();
        currentRunInputs = Array.isArray(runInputs)
          ? runInputs.filter((input): input is number => typeof input === "number")
          : [];
        for (const input of currentRunInputs) foreignSubmissions.add(input);
        for (const item of Array.isArray(inbox) ? inbox : []) {
          const queued = recordNumber(item, "id");
          if (queued !== undefined) foreignSubmissions.add(queued);
        }
        // A rebind during this session's own turn keeps that turn's work.
        for (const own of activeTurn?.submissions.keys() ?? []) foreignSubmissions.delete(own);
        recomputeRunIsForeign();
        boundSnapshot = snapshot;
        liveAtBind = new Set(foreignSubmissions);
        foreignBacklog = [];
        yield* pumpEvents(queue).pipe(Effect.forkIn(nextScope));
        yield* Scope.addFinalizer(
          nextScope,
          worker.request("conversation.unwatch", { conversationId: id }).pipe(Effect.ignore),
        );
        watchScope = nextScope;
        conversationId = id;
        // Anything of the active turn that settled before this watch existed
        // reaches no event pump; read its record directly.
        const unsettled = Array.from(activeTurn?.submissions ?? []).flatMap(
          ([submissionId, record]) => (record === null ? [submissionId] : []),
        );
        for (const submissionId of unsettled) {
          const status = yield* call("submission.status", { submissionId });
          yield* permit.withPermits(1)(
            handleEvent({ type: "submission", record: recordField(status, "record") }),
          );
        }
        if (foreignSubmissions.size > 0) {
          // The previous owner left work that the new owner resumed (S01-E05).
          yield* Effect.logWarning("Pi-Durable conversation resumed work no T3 turn owns", {
            conversationId: id,
            submissions: Array.from(foreignSubmissions),
          });
        }
      });

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        const existing = threadInput.existingProviderThread;
        const existingRef = existing?.nativeThreadRef?.nativeId ?? null;
        let opened: unknown;
        if (existingRef !== null) {
          const parsed = parseDurableThreadRef(existingRef);
          if (parsed === undefined) {
            return yield* protocolError(
              "this thread's Pi session is not a durable conversation; Pi CLI sessions are not resumed by the durable runtime",
            );
          }
          if (parsed.storeId !== worker.status.storeId) {
            return yield* protocolError(
              `this thread's durable conversation belongs to store ${parsed.storeId}, not ${worker.status.storeId}`,
            );
          }
          opened = yield* call("conversation.open", { conversationId: parsed.conversationId });
        } else {
          const slug = resolveSlug(threadInput.modelSelection.model);
          if (slug === null) {
            return yield* protocolError("the durable worker has no model to use as Pi's default");
          }
          const model = parsePiModelSlug(slug);
          if (model === null) {
            return yield* protocolError(`Pi model '${slug}' must use provider/model format`);
          }
          opened = yield* call("conversation.open", {
            cwd: threadInput.runtimePolicy.cwd ?? cwd,
            model,
          });
          appliedModel = slug;
        }
        const id = recordNumber(opened, "conversationId");
        const nativeId = recordString(opened, "nativeThreadRef");
        if (id === undefined || nativeId === undefined) {
          return yield* protocolError("worker returned no conversation", opened);
        }
        yield* bindConversation(id);
        const createdAt = yield* DateTime.now;
        providerThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                status: activeTurn === null ? "idle" : "active",
                updatedAt: createdAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver: PI_PROVIDER,
                  nativeThreadId: nativeId,
                }),
                driver: PI_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: activeTurn === null ? "idle" : "active",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt,
                updatedAt: createdAt,
              };
        yield* emit({ type: "provider_thread.updated", driver: PI_PROVIDER, providerThread });
        return providerThread;
      });

      const applySelection = Effect.fnUntraced(function* (selection: ModelSelection) {
        if (conversationId === null) return;
        const thinking = getModelSelectionStringOptionValue(selection, "thinking");
        const change: PiRpcRecord = {};
        const target = resolveSlug(selection.model);
        if (target === null) {
          return yield* protocolError("the durable worker has no model to use as Pi's default");
        }
        if (target !== appliedModel) {
          const parsed = parsePiModelSlug(target);
          if (parsed === null) {
            return yield* protocolError(`Pi model '${target}' must use provider/model format`);
          }
          change["model"] = parsed;
        }
        if (thinking !== undefined && thinking !== appliedThinking)
          change["thinkingLevel"] = thinking;
        if (Object.keys(change).length === 0) return;
        yield* call("conversation.configure", { conversationId, ...change });
        if (change["model"] !== undefined) {
          appliedModel = target;
          sessionEntity = {
            ...sessionEntity,
            model: selection.model,
            updatedAt: yield* DateTime.now,
          };
          yield* emit({
            type: "provider_session.updated",
            driver: PI_PROVIDER,
            providerSession: sessionEntity,
          });
        }
        if (thinking !== undefined) appliedThinking = thinking;
      });

      /**
       * Projects the run a reattached turn adopts (W02): what it wrote before
       * this session watched, from the watch snapshot, then its output since,
       * from the backlog. Item IDs follow the same order as live mapping, so
       * items already projected before the restart are updated, not repeated.
       *
       * The turn also takes every submission the watch found live in the
       * conversation: the worker kept only this run's conversation, so those
       * are the steers it accepted before the restart, and the turn must not
       * end before they settle.
       */
      const adoptRun = Effect.fnUntraced(function* (turn: ActiveDurableTurn, record: unknown) {
        const startedAt = yield* DateTime.now;
        // A steer that settled after the watch began has left the foreign set
        // for the early settlements; it is still this turn's.
        for (const steer of liveAtBind) {
          if (turn.submissions.has(steer)) continue;
          turn.submissions.set(steer, earlySettlements.get(steer) ?? null);
          earlySettlements.delete(steer);
        }
        foreignSubmissions.clear();
        recomputeRunIsForeign();
        const owned = (inputs: ReadonlyArray<unknown>) =>
          inputs.some((input) => typeof input === "number" && turn.submissions.has(input));
        const calls = new Map<string, { readonly name: string; readonly args: unknown }>();
        const noteCalls = (content: unknown) => {
          for (const block of Array.isArray(content) ? content : []) {
            const id = recordString(block, "id");
            if (recordString(block, "type") === "toolCall" && id !== undefined) {
              calls.set(id, {
                name: recordString(block, "name") ?? "tool",
                args: recordField(block, "arguments"),
              });
            }
          }
        };
        const userEntry = recordNumber(record, "entry");
        const entries = recordField(boundSnapshot, "entries");
        for (const entry of Array.isArray(entries) ? entries : []) {
          const entryId = recordNumber(entry, "id");
          if (userEntry === undefined || entryId === undefined || entryId <= userEntry) continue;
          const message = (recordField(entry, "model") as unknown[] | undefined)?.[0];
          const role = recordString(message, "role");
          const content = recordField(message, "content");
          if (role === "assistant") {
            turn.messageOrdinal += 1;
            noteCalls(content);
            for (const [index, block] of (Array.isArray(content) ? content : []).entries()) {
              const parsed = blockText(block);
              if (parsed === null) continue;
              const item = yield* streamItem(turn, parsed.kind, index);
              item.text = parsed.text;
            }
            yield* completeStreamItems(turn);
            if (recordString(message, "stopReason") === "error") {
              turn.failure = makeProviderFailure({
                message: recordString(message, "errorMessage") ?? "The model returned an error.",
              });
            }
          } else if (role === "toolResult") {
            const callId = recordString(message, "toolCallId") ?? "";
            const call = calls.get(callId);
            const output = contentText(content);
            yield* emitTool(
              turn,
              {
                nativeItemId: `${turn.providerTurn.id}:tool:${callId}`,
                toolName: call?.name ?? recordString(message, "toolName") ?? "tool",
                args: call?.args,
                output,
                startedAt,
              },
              recordField(message, "isError") === true ? "failed" : "completed",
            );
          }
        }
        // The answer and tools in flight when the snapshot was taken.
        const snapshotRun = recordField(recordField(boundSnapshot, "run"), "inputs");
        if (Array.isArray(snapshotRun) && owned(snapshotRun)) {
          const partial = recordField(recordField(boundSnapshot, "generation"), "message");
          if (recordString(partial, "role") === "assistant") {
            turn.messageOrdinal += 1;
            const content = recordField(partial, "content");
            noteCalls(content);
            for (const [index, block] of (Array.isArray(content) ? content : []).entries()) {
              yield* setBlockText(turn, index, block);
            }
          }
          const slots = recordField(boundSnapshot, "tools");
          for (const slot of Array.isArray(slots) ? slots : []) {
            const callId = recordString(slot, "callId");
            if (callId === undefined || recordString(slot, "status") === "done") continue;
            const tool: ToolState = {
              nativeItemId: `${turn.providerTurn.id}:tool:${callId}`,
              toolName: recordString(slot, "name") ?? "tool",
              args: calls.get(callId)?.args,
              output: recordString(slot, "output") ?? "",
              startedAt,
            };
            turn.tools.set(callId, tool);
            yield* emitTool(turn, tool, "running");
          }
        }
        const backlog = foreignBacklog;
        foreignBacklog = [];
        for (const { runInputs, event } of backlog) {
          if (owned(runInputs)) yield* handleOutput(turn, event);
        }
      });

      /**
       * Admit one input for `turn`, whose admission the caller reserved under
       * `permit` in the same critical section that found the turn active. The
       * turn cannot end while a reservation is open, so a settlement that lands
       * during the call cannot orphan the new submission's answer.
       */
      const admitReserved = Effect.fnUntraced(function* (
        turn: ActiveDurableTurn,
        message: ProviderAdapter.ProviderAdapterV2TurnMessage,
        whenBusy: "steer" | "followUp",
        adopt = false,
        /**
         * Runs under the permit when the admission failed, before the turn may
         * settle: anything it emits still belongs to the turn.
         */
        onRefused?: (cause: unknown) => Effect.Effect<void>,
      ) {
        const admitted = yield* Effect.gen(function* () {
          // Stop and this check share the permit: a stopped turn admits nothing new.
          if (yield* permit.withPermits(1)(Effect.sync(() => turn.interrupted))) {
            return yield* protocolError("the turn was stopped before this message was admitted");
          }
          // The worker accepts text only; images and files are parity work (W07).
          if (message.attachments.length > 0) {
            return yield* protocolError(
              "the durable runtime does not accept attachments yet; send the message without them",
            );
          }
          // The request ID makes a repeated submit return the original
          // submission, so a lost reply is recovered by asking again.
          const result = yield* call(
            "conversation.submit",
            {
              conversationId,
              requestId: `t3:${turn.turnInput.threadId}:${message.messageId}`,
              text: message.text,
              whenBusy,
            },
            true,
          );
          const submissionId = recordNumber(result, "submissionId");
          if (submissionId === undefined) {
            return yield* protocolError("worker returned no submission", result);
          }
          // An adopted submission may have settled before this session
          // watched, and its record names the input's transcript entry. A
          // repeated request ID can also return a submission that already
          // settled: its settlement event will not come again.
          const replyStatus = recordString(result, "status");
          const record =
            adopt || replyStatus === "done" || replyStatus === "unanswered"
              ? recordField(yield* call("submission.status", { submissionId }, true), "record")
              : undefined;
          return { submissionId, record };
        }).pipe(Effect.exit);
        yield* permit.withPermits(1)(
          Effect.gen(function* () {
            turn.pendingAdmissions -= 1;
            if (admitted._tag === "Success") {
              const { submissionId, record } = admitted.value;
              // A rebind during the call can have seen this ID as foreign, and
              // an adopted submission was: its run is this turn's now.
              foreignSubmissions.delete(submissionId);
              recomputeRunIsForeign();
              const early = earlySettlements.get(submissionId);
              earlySettlements.delete(submissionId);
              const status = recordString(record, "status");
              const settled =
                status === "done" || status === "unanswered" ? (record as PiRpcRecord) : null;
              // A resubmitted ID the turn already holds keeps a record that
              // settled while the call was in flight.
              turn.submissions.set(
                submissionId,
                early ?? settled ?? turn.submissions.get(submissionId) ?? null,
              );
              if (adopt) yield* adoptRun(turn, record);
            } else if (onRefused !== undefined && activeTurn === turn) {
              yield* onRefused(Cause.squash(admitted.cause));
            }
            if (activeTurn === turn) yield* settleIfDone(turn);
          }),
        );
        // Stop landed while this submit was in flight: the abort may have
        // reached the worker first, so abort again now that the input exists.
        if (admitted._tag === "Success" && turn.interrupted) {
          yield* call("conversation.abort", { conversationId }).pipe(Effect.ignore);
        }
        return (yield* admitted).submissionId;
      });

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          sessionEntity = { ...sessionEntity, status: "stopped", updatedAt: yield* DateTime.now };
          yield* Queue.offer(events, {
            type: "provider_session.updated",
            driver: PI_PROVIDER,
            providerSession: sessionEntity,
          });
          yield* Queue.end(events);
        }),
      );

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: PI_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        getModelContextWindow: (selection) =>
          selection.instanceId === options.instanceId
            ? (contextWindowFor(resolveSlug(selection.model)) ?? undefined)
            : undefined,
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: PI_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            // The worker kept this conversation's work for the reattach; the
            // run fails after a final failed resume, so stop the work too.
            Effect.tapError(() => {
              const kept = parseDurableThreadRef(
                threadInput.providerThread.nativeThreadRef?.nativeId ?? "",
              );
              return threadInput.reattach?.finalAttempt === true &&
                kept !== undefined &&
                kept.storeId === worker.status.storeId
                ? worker
                    .request("conversation.abort", { conversationId: kept.conversationId })
                    .pipe(Effect.ignore)
                : Effect.void;
            }),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: PI_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            if (providerThread === null || conversationId === null) {
              return yield* protocolError("durable session has no registered thread");
            }
            if (activeTurn !== null) {
              return yield* protocolError(
                `durable provider thread ${turnInput.providerThread.id} already has an active turn`,
              );
            }
            if (
              providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError("durable turn requested for a different conversation");
            }
            const adopt = turnInput.reattach !== undefined;
            const adoptedSteers = turnInput.reattach?.steers ?? [];
            // Nothing enforces approvals in the durable runtime yet, so a
            // workspace-mutating tool profile runs only in Full access. Work
            // being adopted already runs under the mode it started in.
            if (
              !adopt &&
              worker.tools === "coding" &&
              turnInput.runtimePolicy.runtimeMode !== "full-access"
            ) {
              return yield* protocolError(
                "the durable runtime cannot ask for approvals yet; switch this thread to Full access or run the worker with a read-only tool profile",
              );
            }
            if (turnInput.message.attachments.length > 0) {
              return yield* protocolError(
                "the durable runtime does not accept attachments yet; send the message without them",
              );
            }
            providerThread = turnInput.providerThread;
            if (adopt) {
              // The run already executes with the selection it started with.
              appliedModel = resolveSlug(turnInput.modelSelection.model);
              appliedThinking =
                getModelSelectionStringOptionValue(turnInput.modelSelection, "thinking") ?? null;
            } else {
              yield* applySelection(turnInput.modelSelection);
            }
            // An adopted turn keeps the start it had before the restart.
            const startedAt = turnInput.reattach?.startedAt ?? (yield* DateTime.now);
            const syntheticNativeTurnId = `${providerThread.id}:attempt:${turnInput.attemptId}`;
            const turn: ActiveDurableTurn = {
              turnInput,
              providerTurn: {
                id: idAllocator.derive.providerTurn({
                  driver: PI_PROVIDER,
                  nativeTurnId: syntheticNativeTurnId,
                }),
                providerThreadId: turnInput.providerThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt,
                completedAt: null,
              },
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              messageOrdinal: 0,
              streamItems: new Map(),
              tools: new Map(),
              submissions: new Map(),
              pendingAdmissions: 0,
              interrupted: false,
              failure: null,
              lastLiveUsedTokens: null,
            };
            // Installed before admission: the answer can stream before submit returns.
            yield* permit.withPermits(1)(
              Effect.gen(function* () {
                activeTurn = turn;
                // A reattached turn also reserves its steers' admissions, so it
                // cannot end on its input before they are admitted.
                turn.pendingAdmissions = 1 + adoptedSteers.length;
                // Only a reattached turn adopts output from before it started.
                if (!adopt) foreignBacklog = [];
                yield* emit({
                  type: "provider_turn.updated",
                  driver: PI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerTurn: turn.providerTurn,
                });
                yield* updateProviderThread({
                  status: "active",
                  firstRunOrdinal: providerThread?.firstRunOrdinal ?? turnInput.runOrdinal,
                  lastRunOrdinal: turnInput.runOrdinal,
                });
                yield* updateProviderSession("running", null);
              }),
            );
            // A reattached turn resubmits its original request ID, which
            // returns the submission admitted before the restart.
            const admittedInput = admitReserved(turn, turnInput.message, "followUp", adopt).pipe(
              Effect.tapError(() =>
                permit.withPermits(1)(
                  Effect.gen(function* () {
                    if (activeTurn !== turn) return;
                    activeTurn = null;
                    // The running update is already out; close it, not just the run.
                    yield* emit({
                      type: "provider_turn.updated",
                      driver: PI_PROVIDER,
                      threadId: turnInput.threadId,
                      providerTurn: {
                        ...turn.providerTurn,
                        status: "failed",
                        completedAt: yield* DateTime.now,
                      },
                    });
                    yield* updateProviderThread({ status: "idle" });
                    yield* updateProviderSession("ready", null);
                  }),
                ),
              ),
            );
            yield* admittedInput;
            // Steers T3 accepted for this turn. Their request IDs return the
            // submissions the provider already holds; a steer the restart cut
            // off is admitted now, into this turn.
            for (const steer of adoptedSteers) {
              // After a Stop nothing more is owed. Otherwise the message stays
              // in the thread, so the turn says it was not delivered rather
              // than drop it silently; the run goes on.
              const notDelivered = (cause: unknown) =>
                turn.interrupted
                  ? Effect.void
                  : Effect.gen(function* () {
                      yield* Effect.logWarning(
                        "A steer accepted before the restart was not admitted",
                        { messageId: steer.messageId, cause },
                      );
                      const at = yield* DateTime.now;
                      const itemId = `${turn.providerTurn.id}:steer-not-delivered:${steer.messageId}`;
                      yield* emit({
                        type: "turn_item.updated",
                        driver: PI_PROVIDER,
                        turnItem: {
                          ...baseItemFields(turn, itemId, at, at),
                          status: "failed",
                          title: "Message not delivered",
                          completedAt: at,
                          type: "error",
                          failure: makeProviderFailure({
                            cause,
                            message:
                              "A message sent just before the server restarted could not be delivered to the agent. Send it again.",
                          }),
                        },
                      });
                    });
              yield* admitReserved(
                turn,
                steer,
                runIsForeign ? "followUp" : "steer",
                false,
                notDelivered,
              ).pipe(Effect.ignore);
            }
          }).pipe(
            // The worker kept this conversation's work for the reattach. When
            // the turn cannot adopt it, T3 fails the run, so stop the work
            // instead of leaving it running with nothing tracking it.
            Effect.tapError(() =>
              turnInput.reattach !== undefined && activeTurn === null && conversationId !== null
                ? call("conversation.abort", { conversationId }).pipe(Effect.ignore)
                : Effect.void,
            ),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: PI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput) =>
          Effect.gen(function* () {
            // Find the turn active and reserve its admission in one critical
            // section, so the event pump cannot end it in between.
            const turn = yield* permit.withPermits(1)(
              Effect.suspend(() => {
                const current = activeTurn;
                if (current === null || current.providerTurn.id !== steerInput.providerTurnId) {
                  return Effect.fail(
                    protocolError(`durable turn ${steerInput.providerTurnId} is not active`),
                  );
                }
                current.pendingAdmissions += 1;
                return Effect.succeed(current);
              }),
            );
            // Steering goes into whichever run is executing. While that is
            // resumed work no T3 turn owns, queue the message behind this
            // turn's own input instead; the turn waits for both.
            yield* admitReserved(turn, steerInput.message, runIsForeign ? "followUp" : "steer");
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver: PI_PROVIDER,
                  providerThreadId: steerInput.providerThread.id,
                  providerTurnId: steerInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        interruptTurn: (interruptInput) =>
          // Find the turn, mark it stopped and abort in one permit section:
          // no settlement or new turn can slip in, so a stale Stop never
          // aborts the turn that follows. The worker's abort does not need
          // this adapter's event pump, which resumes once the permit is free.
          permit
            .withPermits(1)(
              Effect.gen(function* () {
                const turn = activeTurn;
                if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
                  // Nothing of a settled turn is left to stop. A runtime restart
                  // is never needed: the durable store outlives this session.
                  if (interruptInput.requestRuntimeRestart === true) return;
                  return yield* protocolError(
                    `durable turn ${interruptInput.providerTurnId} is not active`,
                  );
                }
                turn.interrupted = true;
                // Resolves once the conversation is idle; settlement events end the turn.
                yield* call("conversation.abort", { conversationId }).pipe(
                  Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))),
                );
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: PI_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: PI_PROVIDER,
              requestId: requestInput.requestId,
              cause: protocolError("the durable runtime raises no runtime requests yet"),
            }),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.gen(function* () {
            const wanted = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            const parsed = wanted == null ? undefined : parseDurableThreadRef(wanted);
            if (parsed === undefined || parsed.storeId !== worker.status.storeId) {
              return yield* protocolError(
                "snapshot requested for a thread this store does not hold",
              );
            }
            const data = yield* call("conversation.entries", {
              conversationId: parsed.conversationId,
            });
            const entries = recordField(data, "entries");
            const threadId = snapshotInput.providerThread.appThreadId ?? input.threadId;
            const messages = (Array.isArray(entries) ? entries : []).flatMap((entry) => {
              const message = (recordField(entry, "model") as unknown[] | undefined)?.[0];
              const role = recordString(message, "role");
              if (role !== "user" && role !== "assistant") return [];
              const text = contentText(recordField(message, "content"));
              if (text.length === 0) return [];
              const at = Option.getOrElse(
                DateTime.make(recordNumber(message, "timestamp") ?? Number.NaN),
                () => snapshotInput.providerThread.createdAt,
              );
              return [
                {
                  id: idAllocator.derive.messageFromProviderItem({
                    driver: PI_PROVIDER,
                    nativeItemId: `${wanted}:entry:${recordNumber(entry, "id") ?? "?"}`,
                  }),
                  threadId,
                  runId: null,
                  nodeId: null,
                  role: role as "user" | "assistant",
                  text,
                  attachments: [],
                  streaming: false,
                  createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                  creationSource: "provider" as const,
                  createdAt: at,
                  updatedAt: at,
                },
              ];
            });
            return {
              providerThread: providerThread ?? snapshotInput.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver: PI_PROVIDER,
                  providerThreadId: snapshotInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        rollbackThread: (rollbackInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: PI_PROVIDER,
              providerThreadId: rollbackInput.providerThread.id,
              cause: protocolError("the durable runtime does not support rollback yet"),
            }),
          ),
        forkThread: (forkInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: PI_PROVIDER,
              providerThreadId: forkInput.sourceProviderThread.id,
              cause: protocolError("the durable runtime does not support fork yet"),
            }),
          ),
      };
      return runtime;
    }),
  });
}
