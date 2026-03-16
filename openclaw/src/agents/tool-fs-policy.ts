import type { OpenClawConfig } from "../config/config.js";
import { resolveAgentConfig } from "./agent-scope.js";

export type ToolFsPolicy = {
  workspaceOnly: boolean;
  readOnlyRoots?: string[];
};

function normalizeReadOnlyRoots(readOnlyRoots: unknown): string[] {
  if (!Array.isArray(readOnlyRoots)) {
    return [];
  }
  const seen = new Set<string>();
  for (const entry of readOnlyRoots) {
    if (typeof entry !== "string") {
      continue;
    }
    const value = entry.trim();
    if (!value) {
      continue;
    }
    seen.add(value);
  }
  return Array.from(seen);
}

export function createToolFsPolicy(params: {
  workspaceOnly?: boolean;
  readOnlyRoots?: string[];
}): ToolFsPolicy {
  return {
    workspaceOnly: params.workspaceOnly === true,
    readOnlyRoots: normalizeReadOnlyRoots(params.readOnlyRoots),
  };
}

export function resolveToolFsConfig(params: { cfg?: OpenClawConfig; agentId?: string }): {
  workspaceOnly?: boolean;
  readOnlyRoots?: string[];
} {
  const cfg = params.cfg;
  const globalFs = cfg?.tools?.fs;
  const agentFs =
    cfg && params.agentId ? resolveAgentConfig(cfg, params.agentId)?.tools?.fs : undefined;
  const readOnlyRoots = agentFs?.readOnlyRoots ?? globalFs?.readOnlyRoots;
  return {
    workspaceOnly: agentFs?.workspaceOnly ?? globalFs?.workspaceOnly,
    readOnlyRoots: Array.isArray(readOnlyRoots)
      ? readOnlyRoots.filter((entry): entry is string => typeof entry === "string")
      : undefined,
  };
}

export function resolveEffectiveToolFsWorkspaceOnly(params: {
  cfg?: OpenClawConfig;
  agentId?: string;
}): boolean {
  return resolveToolFsConfig(params).workspaceOnly === true;
}
