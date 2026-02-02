"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { fetchSchema, listJobs, JobSummary, SchemaSummary } from "@/lib/api";

type SchemaDetailPageProps = {
  params: { schemaId: string };
};

export default function SchemaDetailPage({ params }: SchemaDetailPageProps) {
  const [schema, setSchema] = useState<SchemaSummary | null>(null);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [schemaResponse, jobsResponse] = await Promise.all([
          fetchSchema(params.schemaId),
          listJobs(),
        ]);
        setSchema(schemaResponse.schema);
        setJobs(jobsResponse.jobs ?? []);
        setError(null);
      } catch {
        setSchema(null);
        setJobs([]);
        setError("Unable to load schema details.");
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, [params.schemaId]);

  const schemaJobs = useMemo(
    () => jobs.filter((job) => job.schemaId === params.schemaId),
    [jobs, params.schemaId]
  );

  return (
    <AppShell>
      <div className="flex flex-col gap-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              Schema
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              {schema?.name ?? "Schema details"}
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              {schema
                ? `Version ${schema.version} • Updated ${schema.updatedAt}`
                : "Review mappings and run ingestion jobs."}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Link
              href={`/app/jobs/new?schema=${params.schemaId}`}
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
            >
              New ingestion
            </Link>
            <Link
              href="/app/schemas"
              className="text-sm font-semibold text-slate-600 hover:text-slate-900"
            >
              Back to schemas
            </Link>
          </div>
        </div>

        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-slate-900">
              Ingestion jobs
            </h2>
            <span className="text-sm text-slate-500">
              {schemaJobs.length} total
            </span>
          </div>
          <div className="mt-6 divide-y divide-slate-200">
            {loading && (
              <div className="py-6 text-sm text-slate-500">
                Loading schema jobs...
              </div>
            )}
            {!loading && schemaJobs.length === 0 && (
              <div className="py-6 text-sm text-slate-500">
                No ingestion jobs for this schema yet.
              </div>
            )}
            {schemaJobs.map((job) => (
              <div
                key={job.id}
                className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between"
              >
                <div>
                  <p className="text-sm font-semibold text-slate-900">
                    {job.name}
                  </p>
                  <p className="text-xs text-slate-500">
                    {job.sourceType.toUpperCase()} • {job.createdAt}
                  </p>
                </div>
                <Link
                  href={`/app/jobs/${job.id}`}
                  className="text-sm font-semibold text-slate-900 hover:text-slate-600"
                >
                  View job →
                </Link>
              </div>
            ))}
          </div>
          {error && <p className="mt-4 text-xs text-rose-600">{error}</p>}
        </div>
      </div>
    </AppShell>
  );
}
