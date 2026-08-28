/**
 * FxAcpSupport - helpers for Fx ACP sessions and model selection.
 *
 * Owns spawn input construction and ACP config mutations used by the Fx provider adapter.
 *
 * @module FxAcpSupport
 */
import { Effect, Layer, Scope, ServiceMap } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as AcpErrors from "./AcpErrors.ts";
import type * as Acp from "@agentclientprotocol/sdk";

import { buildProviderChildEnvironment } from "../../providerChildEnvironment.ts";
import {
  AcpSessionRuntime,
  type AcpSessionRuntimeOptions,
  type AcpSessionRuntimeShape,
  type AcpSpawnInput,
} from "./AcpSessionRuntime.ts";

export interface FxAcpRuntimeSettings {
  readonly binaryPath?: string;
  readonly model?: string;
}

export interface FxAcpRuntimeInput extends Omit<
  AcpSessionRuntimeOptions,
  "authMethodId" | "resolveAuthMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly fxSettings: FxAcpRuntimeSettings | null | undefined;
}

export interface FxAcpModelSelectionErrorContext {
  readonly cause: AcpErrors.AcpError;
  readonly method: "session/set_config_option";
}

const FX_MODEL_CONFIG_ID = "model";
const FX_MODE_CONFIG_ID = "mode";

export function buildFxAcpSpawnInput(
  fxSettings: FxAcpRuntimeSettings | null | undefined,
  cwd: string,
): AcpSpawnInput {
  const args: string[] = ["acp"];
  // Fx process-level --model override takes precedence over session model.
  // Only use when explicitly set via settings; session set_config_option is primary.
  if (fxSettings?.model?.trim()) {
    args.unshift("--model", fxSettings.model.trim());
  }
  return {
    command: fxSettings?.binaryPath?.trim() || "fx",
    args,
    cwd,
    env: buildProviderChildEnvironment({ provider: "fx" }),
  };
}

export const makeFxAcpRuntime = (
  input: FxAcpRuntimeInput,
): Effect.Effect<AcpSessionRuntimeShape, AcpErrors.AcpError, Scope.Scope> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildFxAcpSpawnInput(input.fxSettings, input.cwd),
        // Fx advertises authMethods: [] (no authenticate step).
        // AcpSessionRuntime now skips authenticate when no methods are advertised.
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return ServiceMap.getUnsafe(acpContext, AcpSessionRuntime);
  });

export function applyFxAcpModelSelection<E>(input: {
  readonly runtime: Pick<AcpSessionRuntimeShape, "setConfigOption">;
  readonly model: string;
  readonly mapError: (context: FxAcpModelSelectionErrorContext) => E;
}): Effect.Effect<void, E> {
  return Effect.gen(function* () {
    const model = input.model.trim();
    if (!model) {
      return;
    }
    yield* input.runtime
      .setConfigOption(FX_MODEL_CONFIG_ID, model)
      .pipe(Effect.mapError((cause) => input.mapError({ cause, method: "session/set_config_option" })));
  });
}

export function applyFxAcpModeSelection<E>(input: {
  readonly runtime: Pick<AcpSessionRuntimeShape, "setConfigOption" | "setMode">;
  readonly modeId: string;
  readonly mapError: (context: FxAcpModelSelectionErrorContext) => E;
}): Effect.Effect<void, E> {
  // Fx exposes mode as a select config option ("ask" / "code") and also via set_mode.
  // Prefer setMode when available, fall back to set_config_option.
  return input.runtime.setMode(input.modeId).pipe(
    Effect.catch(() => input.runtime.setConfigOption(FX_MODE_CONFIG_ID, input.modeId)),
    Effect.mapError((cause) => input.mapError({ cause, method: "session/set_config_option" })),
    Effect.asVoid,
  );
}

export function resolveFxModeId(input: {
  readonly interactionMode?: string;
  readonly runtimeMode?: string;
}): string {
  if (input.interactionMode === "plan") {
    return "ask";
  }
  if (input.runtimeMode === "full-access") {
    return "code";
  }
  // approval-required / auto -> ask
  return "ask";
}
