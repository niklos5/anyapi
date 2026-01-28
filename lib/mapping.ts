export type MappingEntry = {
  source: string | string[];
  target: string;
  transform?: "string" | "number" | "integer" | "boolean" | "date";
  required?: boolean;
  default?: unknown;
  match?: Record<string, unknown>;
};

export type MappingSpec = {
  targetSchema?: unknown;
  mappings: MappingEntry[];
  defaults?: Record<string, unknown>;
};

export const parseMappingSpec = (value: string): MappingSpec | null => {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    if (!Array.isArray(parsed.mappings)) {
      return null;
    }
    return parsed as MappingSpec;
  } catch {
    return null;
  }
};
