/**
 * PiDurableInstance — the `pi` provider instance when its config carries
 * `durable` (T3.14). The orchestration adapter talks to the box-side
 * Pi-Durable worker; the snapshot lists the worker's models. Nothing here runs
 * the `pi` CLI: there is no CLI probe, no CLI text generation and no
 * self-update, because the worker, not Pi's installer, owns the runtime.
 */
import {
  ProviderDriverKind,
  TextGenerationError,
  type PiDurableSettings,
  type PiSettings,
  type RuntimeMode,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as ServerSettings from "../../serverSettings.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import * as DurableReattach from "../../orchestration-v2/DurableReattach.ts";
import {
  DURABLE_INSPECTION_RELEASE_HINT,
  makeDurableWorkerManager,
  makePiDurableAdapterV2,
  PI_INHERIT_MODEL_SLUG,
  type DurableWorkerModel,
  type DurableWorkerStatus,
} from "../../orchestration-v2/Adapters/PiDurableAdapterV2.ts";
import * as ServerConfig from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { thinkingCapabilitiesForPiModel } from "../Layers/piThinkingCapabilities.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriverCreateInput,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { buildServerProvider } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const DRIVER_KIND = ProviderDriverKind.make("pi");
const MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

const PRESENTATION = {
  displayName: "Pi",
  showInteractionModeToggle: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
} as const;

type ProviderProbe = Parameters<typeof buildServerProvider>[0]["probe"];

/**
 * Health and model catalog for a running worker. The catalog leads with Pi's
 * "default" entry, which the adapter resolves to the worker's default model,
 * so saved `default` selections keep their meaning.
 */
export function durableProviderProbe(input: {
  readonly status: DurableWorkerStatus;
  /** The worker's tool profile; `coding` mutates the workspace. */
  readonly tools: string;
  readonly models: ReadonlyArray<DurableWorkerModel>;
  readonly defaultModel: { readonly provider: string; readonly modelId: string } | null;
}): {
  readonly probe: ProviderProbe;
  readonly models: ServerProvider["models"];
  readonly supportedRuntimeModes: ReadonlyArray<RuntimeMode>;
} {
  const enabled = input.status.scheduling === "enabled";
  const defaultModel =
    input.defaultModel === null
      ? undefined
      : input.models.find(
          (model) =>
            model.provider === input.defaultModel?.provider &&
            model.modelId === input.defaultModel.modelId,
        );
  const defaultName =
    input.defaultModel === null
      ? null
      : (defaultModel?.name ?? `${input.defaultModel.provider}/${input.defaultModel.modelId}`);
  // Pi's own derivation: reasoning models offer the thinking levels pi-ai supports.
  const capabilities = (model: DurableWorkerModel | undefined) =>
    thinkingCapabilitiesForPiModel(model, undefined);
  return {
    // Nothing asks for approvals in the durable runtime yet, so a worker that
    // can mutate the workspace runs only in Full access (the adapter refuses
    // other modes); read-only and tool-free workers run in any mode.
    supportedRuntimeModes:
      input.tools === "coding" ? ["full-access"] : PRESENTATION.supportedRuntimeModes,
    probe: {
      installed: true,
      version: null,
      status: enabled ? "ready" : "warning",
      auth: { status: input.models.length > 0 ? "authenticated" : "unauthenticated" },
      ...(enabled
        ? {}
        : {
            message: `Durable runtime is in inspection (${input.status.reasons.join(", ")}). ${DURABLE_INSPECTION_RELEASE_HINT}`,
          }),
    },
    models: [
      ...(defaultName === null
        ? []
        : [
            {
              slug: PI_INHERIT_MODEL_SLUG,
              name: `Pi default (${defaultName})`,
              isCustom: false,
              capabilities: capabilities(defaultModel),
            },
          ]),
      ...input.models.map((model) => ({
        slug: `${model.provider}/${model.modelId}`,
        name: model.name,
        isCustom: false,
        capabilities: capabilities(model),
      })),
    ],
  };
}

export const makePiDurableProviderInstance = Effect.fnUntraced(function* (
  input: ProviderDriverCreateInput<PiSettings>,
  durable: PiDurableSettings,
) {
  const { instanceId, displayName, accentColor, environment, enabled, config } = input;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const continuationIdentity = defaultProviderContinuationIdentity({
    driverKind: DRIVER_KIND,
    instanceId,
  });
  const reattachPlan = yield* Effect.serviceOption(DurableReattach.DurableReattachPlan);
  const workers = yield* makeDurableWorkerManager({
    launch: durable,
    env: mergeProviderInstanceEnvironment(environment),
    // The worker keeps the conversations of runs startup recovery reattaches.
    ...(Option.isSome(reattachPlan)
      ? { keepConversations: reattachPlan.value.conversationsFor(instanceId) }
      : {}),
  });
  const stamp = (snapshot: Omit<ServerProvider, "instanceId" | "driver">): ServerProvider => ({
    ...snapshot,
    instanceId,
    driver: DRIVER_KIND,
    ...(displayName ? { displayName } : {}),
    ...(accentColor ? { accentColor } : {}),
    continuation: { groupKey: continuationIdentity.continuationKey },
  });

  const describe = (
    probe: Parameters<typeof buildServerProvider>[0]["probe"],
    models = [] as ServerProvider["models"],
    supportedRuntimeModes: ReadonlyArray<RuntimeMode> = PRESENTATION.supportedRuntimeModes,
  ) =>
    Effect.map(DateTime.now, (now) =>
      stamp(
        buildServerProvider({
          presentation: { ...PRESENTATION, supportedRuntimeModes },
          enabled,
          checkedAt: DateTime.formatIso(now),
          models,
          probe,
        }),
      ),
    );

  const checkProvider = enabled
    ? workers.get.pipe(
        // Read the barrier now: an inspection can end between refreshes.
        Effect.flatMap((worker) =>
          worker.currentStatus.pipe(
            Effect.map((status) =>
              durableProviderProbe({
                status,
                tools: worker.tools,
                models: worker.models,
                defaultModel: worker.defaultModel,
              }),
            ),
          ),
        ),
        Effect.flatMap(({ probe, models, supportedRuntimeModes }) =>
          describe(probe, models, supportedRuntimeModes),
        ),
        Effect.catch((error) =>
          describe({
            installed: false,
            version: null,
            status: "error",
            auth: { status: "unknown" },
            message: error.message,
          }),
        ),
      )
    : describe({
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      });

  const settingsSource = makeProviderSnapshotSettingsSource({ ...config, enabled }, serverSettings);
  const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<PiSettings>>({
    resolveMaintenance: () => Effect.succeed(MAINTENANCE),
    getSettings: settingsSource.getSettings,
    streamSettings: settingsSource.streamSettings,
    haveSettingsChanged: haveProviderSnapshotSettingsChanged,
    initialSnapshot: () =>
      describe({
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Starting the Pi-Durable worker...",
      }),
    checkProvider,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderDriverError({
          driver: DRIVER_KIND,
          instanceId,
          detail: "Failed to build the Pi-Durable snapshot.",
          cause,
        }),
    ),
  );

  // Text generation would otherwise spawn the `pi` CLI against real models.
  const unavailable = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Text generation is not available on the Pi-Durable runtime yet.",
      }),
    );

  return {
    instanceId,
    driverKind: DRIVER_KIND,
    continuationIdentity,
    displayName,
    accentColor,
    enabled,
    snapshot,
    orchestrationAdapter: makePiDurableAdapterV2({
      instanceId,
      workers,
      idAllocator,
      serverConfig,
    }),
    textGeneration: {
      generateCommitMessage: () => unavailable("generateCommitMessage"),
      generatePrContent: () => unavailable("generatePrContent"),
      generateBranchName: () => unavailable("generateBranchName"),
      generateThreadTitle: () => unavailable("generateThreadTitle"),
    },
  } satisfies ProviderInstance;
});
