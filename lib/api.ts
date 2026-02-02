import { MappingSpec } from "@/lib/mapping";
import { clearAccessToken, getAccessToken, refresh } from "@/lib/auth";
import { JobStatus, TransformerMetadata } from "@/lib/types";

const BASE_URL =
  process.env.NEXT_PUBLIC_BACKEND_URL ?? "http://localhost:8080";

export type JobSummary = {
  id: string;
  name: string;
  sourceType: string;
  status: JobStatus;
  createdAt: string;
  schemaId?: string;
};

type RequestOptions = {
  method?: string;
  body?: string;
  headers?: Record<string, string>;
  skipAuth?: boolean;
};

const withAuthHeaders = (
  options: RequestOptions = {}
): Record<string, string> => {
  const headers: Record<string, string> = { ...(options.headers ?? {}) };
  const token = getAccessToken();
  if (!options.skipAuth && token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
};

const doRequest = async <T>(
  path: string,
  options: RequestOptions = {},
  hasRetried = false
): Promise<T> => {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: options.method ?? "GET",
    headers: withAuthHeaders(options),
    body: options.body,
  });

  if (response.status === 401 && !options.skipAuth && !hasRetried) {
    try {
      const token = await refresh();
      if (token) {
        return doRequest<T>(path, options, true);
      }
    } catch {
      // fall through
    }
    clearAccessToken();
    if (typeof window !== "undefined") {
      window.location.href = "/login";
    }
  }

  if (!response.ok) {
    throw new Error("Request failed");
  }

  return response.json() as Promise<T>;
};

export const analyzeSource = async (data: unknown) =>
  doRequest<{
    schema: Record<string, string>;
    preview: Record<string, unknown>[];
    issues: { field: string; level: string; message: string }[];
  }>("/analyze", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ data }),
  });

export const createJob = async (payload: {
  name: string;
  sourceType: string;
  data: unknown;
  mapping: MappingSpec;
}) =>
  doRequest<{ job: JobSummary; result?: unknown }>("/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

export const listJobs = async (): Promise<{ jobs: JobSummary[] }> =>
  doRequest("/jobs");

export const fetchJob = async (jobId: string): Promise<JobSummary> =>
  doRequest(`/jobs/${jobId}`);

export const fetchJobResults = async (
  jobId: string
): Promise<{ jobId: string; result: unknown }> =>
  doRequest(`/jobs/${jobId}/results`);

export type SchemaSummary = {
  id: string;
  name: string;
  schemaDefinition: unknown;
  defaultMapping?: MappingSpec;
  metadata?: TransformerMetadata;
  createdAt: string;
  updatedAt: string;
  version: number;
};

export const listSchemas = async (): Promise<{ schemas: SchemaSummary[] }> =>
  doRequest("/schemas");

export const createSchema = async (payload: {
  name: string;
  schemaDefinition: unknown;
  defaultMapping?: MappingSpec;
  metadata?: TransformerMetadata;
}) =>
  doRequest<{ schema: SchemaSummary; apiKey?: string }>("/schemas", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

export const fetchSchema = async (schemaId: string) =>
  doRequest<{ schema: SchemaSummary }>(`/schemas/${schemaId}`);

export const updateSchema = async (
  schemaId: string,
  payload: {
    name?: string;
    schemaDefinition?: unknown;
    defaultMapping?: MappingSpec;
    metadata?: TransformerMetadata;
  }
) =>
  doRequest<{ schema: SchemaSummary }>(`/schemas/${schemaId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

export const ingestSchema = async (schemaId: string, payload: {
  data: unknown;
  name?: string;
  sourceType?: string;
  mapping?: MappingSpec;
}) =>
  doRequest<{ job: JobSummary; result?: unknown }>(
    `/schemas/${schemaId}/ingest`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );
