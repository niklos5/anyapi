import { getAccessToken } from "@/lib/auth";
import { MappingSpec } from "@/lib/mapping";
import { JobStatus } from "@/lib/types";

const BASE_URL =
  process.env.NEXT_PUBLIC_BACKEND_URL ?? "http://localhost:8080";

export type JobSummary = {
  id: string;
  name: string;
  sourceType: string;
  status: JobStatus;
  createdAt: string;
};

const authHeaders = () => {
  const token = getAccessToken();
  if (!token) {
    return {};
  }
  return { Authorization: `Bearer ${token}` };
};

export const analyzeSource = async (data: unknown) => {
  const response = await fetch(`${BASE_URL}/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify({ data }),
  });
  if (!response.ok) {
    throw new Error("Failed to analyze source");
  }
  return response.json();
};

export const createJob = async (payload: {
  name: string;
  sourceType: string;
  data: unknown;
  mapping: MappingSpec;
}) => {
  const response = await fetch(`${BASE_URL}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Failed to create job");
  }
  return response.json();
};

export const listJobs = async (): Promise<{ jobs: JobSummary[] }> => {
  const response = await fetch(`${BASE_URL}/jobs`, {
    method: "GET",
    headers: { ...authHeaders() },
  });
  if (!response.ok) {
    throw new Error("Failed to fetch jobs");
  }
  return response.json();
};

export const fetchJob = async (jobId: string) => {
  const response = await fetch(`${BASE_URL}/jobs/${jobId}`, {
    method: "GET",
    headers: { ...authHeaders() },
  });
  if (!response.ok) {
    throw new Error("Failed to fetch job");
  }
  return response.json();
};

export const fetchJobResults = async (jobId: string) => {
  const response = await fetch(`${BASE_URL}/jobs/${jobId}/results`, {
    method: "GET",
    headers: { ...authHeaders() },
  });
  if (!response.ok) {
    throw new Error("Failed to fetch job results");
  }
  return response.json();
};
