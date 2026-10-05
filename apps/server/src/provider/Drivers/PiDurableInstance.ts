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
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import * as ServerSettings from "../../serverSettings.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import {
  makeDurableWorkerManager,
  makePiDurableAdapterV2,
} from "../../orchestration-v2/Adapters/PiDurableAdapterV2.ts";
import * as ServerConfig from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { EMPTY_PI_MODEL_CAPABILITIES } from "../Layers/piThinkingCapabilities.ts";
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
  const workers = yield* makeDurableWorkerManager({
    launch: durable,
    env: mergeProviderInstanceEnvironment(environment),
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
  ) =>
    Effect.map(DateTime.now, (now) =>
      stamp(
        buildServerProvider({
          presentation: PRESENTATION,
          enabled,
          checkedAt: DateTime.formatIso(now),
          models,
          probe,
        }),
      ),
    );

  const checkProvider = enabled
    ? workers.get.pipe(
        Effect.flatMap((worker) =>
          describe(
            {
              installed: true,
              version: null,
              status: worker.status.scheduling === "enabled" ? "ready" : "warning",
              auth: { status: worker.models.length > 0 ? "authenticated" : "unauthenticated" },
              ...(worker.status.scheduling === "enabled"
                ? {}
                : {
                    message: `Durable runtime is in inspection (${worker.status.reasons.join(", ")}). Release it on the box with \`t3-14 inspect release\`.`,
                  }),
            },
            worker.models.map((model) => ({
              slug: `${model.provider}/${model.modelId}`,
              name: model.name,
              isCustom: false,
              capabilities: EMPTY_PI_MODEL_CAPABILITIES,
            })),
          ),
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
