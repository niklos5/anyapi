"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { listJobs, listSchemas, JobSummary, SchemaSummary } from "@/lib/api";

export default function Home() {
  const [schemas, setSchemas] = useState<SchemaSummary[]>([]);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const [schemasResponse, jobsResponse] = await Promise.all([
          listSchemas(),
          listJobs(),
        ]);
        setSchemas(schemasResponse.schemas ?? []);
        setJobs(jobsResponse.jobs ?? []);
        setError(null);
      } catch {
        setSchemas([]);
        setJobs([]);
        setError("Unable to load jobs right now.");
      } finally {
        setLoading(false);
      }
    };
    loadData();
  }, []);

  const jobsBySchema = useMemo(() => {
    const grouped: Record<string, JobSummary[]> = {};
    for (const job of jobs) {
      if (!job.schemaId) {
        continue;
      }
      if (!grouped[job.schemaId]) {
        grouped[job.schemaId] = [];
      }
      grouped[job.schemaId].push(job);
    }
    return grouped;
  }, [jobs]);

  return (
    <AppShell>
      <section className="flex flex-col gap-6">
        <div className="flex flex-col gap-4 rounded-2xl bg-white p-8 shadow-sm">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
                AnyApi
              </p>
              <h1 className="text-3xl font-semibold text-slate-900">
                Connect data sources and map any schema to any schema.
              </h1>
              <p className="mt-3 max-w-2xl text-base text-slate-600">
                Upload files or connect APIs, let the system analyze your data,
                define a target schema mapping, and track ingestion jobs.
              </p>
            </div>
            <Link
              href="/app/schemas/new"
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
            >
              Deploy schema
            </Link>
          </div>
        </div>

        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-slate-900">
              Mappings (schemas)
            </h2>
            <span className="text-sm text-slate-500">
              {schemas.length} total
            </span>
          </div>
          <div className="mt-6 divide-y divide-slate-200">
            {loading && (
              <div className="py-6 text-sm text-slate-500">
                Loading schemas from backend...
              </div>
            )}
            {!loading && schemas.length === 0 && (
              <div className="py-6 text-sm text-slate-500">
                No schemas deployed yet.
              </div>
            )}
            {schemas.map((schema) => {
              const jobCount = jobsBySchema[schema.id]?.length ?? 0;
              return (
                <div
                  key={schema.id}
                  className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div>
                    <p className="text-sm font-semibold text-slate-900">
                      {schema.name}
                    </p>
                    <p className="text-xs text-slate-500">
                      Version {schema.version} • Updated {schema.updatedAt} •{" "}
                      {jobCount} job{jobCount === 1 ? "" : "s"}
                    </p>
                  </div>
                  <div className="flex items-center gap-3">
                    <Link
                      href={`/app/schemas/${schema.id}`}
                      className="text-sm font-semibold text-slate-900 hover:text-slate-600"
                    >
                      View →
                    </Link>
                  </div>
                </div>
              );
            })}
          </div>
          {error && <p className="mt-4 text-xs text-rose-600">{error}</p>}
        </div>
      </section>
    </AppShell>
  );
}
