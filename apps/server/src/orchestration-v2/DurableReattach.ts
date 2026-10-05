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

import { parseDurableThreadRef } from "./Adapters/PiDurableAdapterV2.ts";
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
 * reattach adopts the settled submission and finishes it. `failed` is left
 * out: the submission may never have been admitted, and resubmitting it would
 * start new work.
 */
export const isAdoptableProviderTurnStatus = (status: string) =>
  status === "running" || status === "completed" || status === "interrupted";

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
      (turn) => turn.runAttemptId === attemptId && isAdoptableProviderTurnStatus(turn.status),
    );
    return adoptable ? [{ run, attemptId, conversation }] : [];
  });
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
  }
>()("t3/orchestration-v2/DurableReattach/DurableReattachPlan") {}

export const make = Effect.gen(function* () {
  const recorded = yield* Deferred.make<ReadonlyArray<DurableReattachPlanEntry>>();
  return DurableReattachPlan.of({
    complete: (entries) => Deferred.succeed(recorded, entries).pipe(Effect.asVoid),
    conversationsFor: (instanceId) =>
      Deferred.await(recorded).pipe(
        Effect.map((entries) =>
          entries.flatMap((entry) => (entry.instanceId === instanceId ? [entry.conversation] : [])),
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
