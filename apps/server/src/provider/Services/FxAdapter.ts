/**
 * FxAdapter - Fx (https://fx.sh) implementation of the generic provider adapter contract.
 *
 * This service owns Fx runtime/session semantics via ACP and emits canonical
 * provider runtime events. It does not perform cross-provider routing.
 *
 * @module FxAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface FxAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "fx";
}

export class FxAdapter extends ServiceMap.Service<FxAdapter, FxAdapterShape>()(
  "synara/provider/Services/FxAdapter",
) {}
