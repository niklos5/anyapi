export type MappingEntry = {
  source: string | string[];
  target: string;
  transform?: "string" | "number" | "integer" | "boolean" | "date";
  required?: boolean;
  default?: unknown;
  match?: Record<string, unknown>;
};

export type RoasterMappingSpec = {
  version?: string;
  defaults?: Record<string, unknown>;
  broadcast?: Record<string, unknown>;
  mappings: {
    items: {
      path: string;
      map: Record<string, unknown>;
    };
  };
};

export type MappingSpec = {
  targetSchema?: unknown;
  mappings: MappingEntry[] | RoasterMappingSpec["mappings"];
  defaults?: Record<string, unknown>;
};

export type MappingAgentOptions = {
  enabled?: boolean;
  maxIterations?: number;
};

export const parseMappingSpec = (value: string): MappingSpec | null => {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    if (Array.isArray(parsed.mappings)) {
      return parsed as MappingSpec;
    }
    if (
      parsed.mappings &&
      typeof parsed.mappings === "object" &&
      typeof parsed.mappings.items === "object"
    ) {
      return parsed as MappingSpec;
    }
    return null;
  } catch {
    return null;
  }
};
