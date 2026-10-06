/**
 * T3.14 W02: a run bound to a Pi-Durable conversation survives a server
 * restart. The durable worker keeps the conversation's work, so startup
 * recovery leaves such a run `running` and enqueues `durable-run.reattach`,
 * which opens a session and adopts the run, instead of cancelling it like
 * other provider work.
 *
 * Recovery decides which conversations to keep before the worker may run
 * anything: the worker boots holding recovered work, and the plan below holds
 * the worker until startup recovery has recorded what it left running.
 */
import type { ProviderInstanceId, RunAttemptId, RunId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";

import { parseDurableThreadRef } from "./Adapters/PiDurableAdapterV2.ts";

export { isTransientDurableFailure } from "./Adapters/PiDurableAdapterV2.ts";
import type { ProjectionRuntimeRecoveryState } from "./ProjectionStore.ts";

export interface DurableConversationRef {
  readonly storeId: string;
  readonly conversationId: number;
}

export interface DurableBoundRun {
  readonly run: ProjectionRuntimeRecoveryState["runs"][number];
  readonly attemptId: RunAttemptId;
  readonly conversation: DurableConversationRef;
}

/**
 * A provider turn whose submission was admitted. A turn that settled just
 * before a crash leaves its run `running` until the run is finalized; the
 * reattach adopts the settled submission and finishes it. A `failed` turn
 * counts only with the strong submission ref the adapter sets once its input
 * was admitted: a failure before admission has none, and resubmitting it
 * would start new work.
 */
export const isAdoptableProviderTurn = (
  turn: Pick<ProjectionRuntimeRecoveryState["providerTurns"][number], "status" | "nativeTurnRef">,
) =>
  turn.status === "running" ||
  turn.status === "completed" ||
  turn.status === "interrupted" ||
  (turn.status === "failed" &&
    turn.nativeTurnRef?.strength === "strong" &&
    turn.nativeTurnRef.nativeId?.startsWith("pi-durable-submission:") === true);

/**
 * Runs the durable worker may still own: `running`, with an adoptable
 * provider turn on the active attempt, on a provider thread bound to a
 * durable conversation.
 */
export function durableBoundRuns(
  projection: Pick<ProjectionRuntimeRecoveryState, "runs" | "providerThreads" | "providerTurns">,
): ReadonlyArray<DurableBoundRun> {
  return projection.runs.flatMap((run) => {
    if (run.status !== "running" || run.activeAttemptId === null) return [];
    const providerThread = projection.providerThreads.find(
      (candidate) => candidate.id === run.providerThreadId,
    );
    const nativeId = providerThread?.nativeThreadRef?.nativeId;
    const conversation =
      providerThread?.driver === "pi" && nativeId != null
        ? parseDurableThreadRef(nativeId)
        : undefined;
    if (conversation === undefined) return [];
    const attemptId = run.activeAttemptId;
    const adoptable = projection.providerTurns.some(
      (turn) => turn.runAttemptId === attemptId && isAdoptableProviderTurn(turn),
    );
    return adoptable ? [{ run, attemptId, conversation }] : [];
  });
}

const isOpenStatus = (status: string) =>
  status === "pending" || status === "running" || status === "waiting";

/**
 * Output a run projected before the restart that is still open: streaming
 * messages, unsettled items and child nodes. A run whose reattach fails
 * settles it, so the failed run shows no live indicator.
 */
export function settledOpenRunOutput(input: {
  readonly runId: RunId;
  readonly rootNodeId: string;
  readonly projection: Pick<ProjectionRuntimeRecoveryState, "messages" | "turnItems" | "nodes">;
  readonly now: DateTime.Utc;
}) {
  const { runId, now } = input;
  return {
    messages: input.projection.messages
      .filter((message) => message.runId === runId && message.streaming)
      .map((message) => ({ ...message, streaming: false, updatedAt: now })),
    turnItems: input.projection.turnItems
      .filter((item) => item.runId === runId && isOpenStatus(item.status))
      .map((item) => ({
        ...item,
        status: "cancelled" as const,
        completedAt: now,
        updatedAt: now,
        ...(item.type === "reasoning" || item.type === "assistant_message"
          ? { streaming: false }
          : {}),
      })),
    nodes: input.projection.nodes
      .filter(
        (node) => node.runId === runId && node.id !== input.rootNodeId && isOpenStatus(node.status),
      )
      .map((node) => ({ ...node, status: "cancelled" as const, completedAt: now })),
  };
}

/**
 * One reattach per run attempt and startup. A later restart enqueues a new
 * one: the row of an earlier startup is settled or cancelled by then.
 */
export const durableReattachEffectId = (
  runId: RunId,
  attemptId: RunAttemptId,
  startedAt: DateTime.Utc,
) => `effect:durable-reattach:${runId}:${attemptId}:${DateTime.formatIso(startedAt)}`;

export interface DurableReattachPlanEntry {
  readonly instanceId: ProviderInstanceId;
  readonly conversation: DurableConversationRef;
}

export class DurableReattachPlan extends Context.Service<
  DurableReattachPlan,
  {
    /** Records what startup recovery left running. The first call wins. */
    readonly complete: (entries: ReadonlyArray<DurableReattachPlanEntry>) => Effect.Effect<void>;
    /** Waits for startup recovery, then lists the conversations to keep for one instance. */
    readonly conversationsFor: (
      instanceId: ProviderInstanceId,
    ) => Effect.Effect<ReadonlyArray<DurableConversationRef>>;
    /** How an instance stops work its worker kept, for as long as the scope lives. */
    readonly registerAbandon: (
      instanceId: ProviderInstanceId,
      abandon: (conversation: DurableConversationRef) => Effect.Effect<void>,
    ) => Effect.Effect<void, never, Scope.Scope>;
    /**
     * Stops kept work no reattach will adopt, such as the work of a run that
     * ended before its reattach ran. The conversation also leaves the plan,
     * so a worker the instance starts later does not keep it.
     */
    readonly abandon: (
      instanceId: ProviderInstanceId,
      conversation: DurableConversationRef,
    ) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/DurableReattach/DurableReattachPlan") {}

export const make = Effect.gen(function* () {
  const recorded = yield* Deferred.make<ReadonlyArray<DurableReattachPlanEntry>>();
  const abandoned = new Set<string>();
  const key = (instanceId: ProviderInstanceId, conversation: DurableConversationRef) =>
    `${instanceId}\u0000${conversation.storeId}\u0000${conversation.conversationId}`;
  const abandoners = new Map<
    ProviderInstanceId,
    (conversation: DurableConversationRef) => Effect.Effect<void>
  >();
  return DurableReattachPlan.of({
    registerAbandon: (instanceId, abandon) =>
      Effect.acquireRelease(
        Effect.sync(() => abandoners.set(instanceId, abandon)),
        () =>
          Effect.sync(() => {
            if (abandoners.get(instanceId) === abandon) abandoners.delete(instanceId);
          }),
      ).pipe(Effect.asVoid),
    abandon: (instanceId, conversation) =>
      Effect.suspend(() => {
        abandoned.add(key(instanceId, conversation));
        return abandoners.get(instanceId)?.(conversation) ?? Effect.void;
      }),
    complete: (entries) => Deferred.succeed(recorded, entries).pipe(Effect.asVoid),
    conversationsFor: (instanceId) =>
      Deferred.await(recorded).pipe(
        Effect.map((entries) =>
          entries.flatMap((entry) =>
            entry.instanceId === instanceId && !abandoned.has(key(instanceId, entry.conversation))
              ? [entry.conversation]
              : [],
          ),
        ),
      ),
  });
});

/**
 * Provided once above both startup recovery and the provider instance
 * registry, so they share one plan. Without it, recovery cancels durable runs
 * as stock T3 does and workers abort their recovered work.
 */
export const layer = Layer.effect(DurableReattachPlan, make);
