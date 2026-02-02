"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import StatusBadge from "@/components/StatusBadge";
import { listJobs, JobSummary } from "@/lib/api";

export default function Home() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadJobs = async () => {
      try {
        const response = await listJobs();
        setJobs(response.jobs ?? []);
        setError(null);
      } catch {
        setJobs([]);
        setError("Unable to load jobs right now.");
      } finally {
        setLoading(false);
      }
    };
    loadJobs();
  }, []);

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
              href="/app/jobs/new"
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
            >
              Create new ingestion
            </Link>
          </div>
        </div>

        <div className="grid gap-6 md:grid-cols-[2fr_1fr]">
          <div className="rounded-2xl bg-white p-6 shadow-sm">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-slate-900">
                Existing ingestion jobs
              </h2>
              <span className="text-sm text-slate-500">
                {jobs.length} total
              </span>
            </div>
            <div className="mt-6 divide-y divide-slate-200">
              {loading && (
                <div className="py-6 text-sm text-slate-500">
                  Loading jobs from backend...
                </div>
              )}
              {!loading && jobs.length === 0 && (
                <div className="py-6 text-sm text-slate-500">
                  No ingestion jobs yet.
                </div>
              )}
              {jobs.map((job) => (
                <div
                  key={job.id}
                  className="flex flex-col gap-4 py-5 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div>
                    <p className="text-sm font-semibold text-slate-900">
                      {job.name}
                    </p>
                    <p className="text-xs text-slate-500">
                      {job.sourceType.toUpperCase()} • {job.createdAt} •{" "}
                      {(() => {
                        const value = (job as unknown as { records?: unknown })
                          .records;
                        return typeof value === "number"
                          ? value.toLocaleString()
                          : "—";
                      })()}{" "}
                      records
                    </p>
                  </div>
                  <div className="flex items-center gap-4">
                    <StatusBadge status={job.status} />
                    <Link
                      href={`/app/jobs/${job.id}`}
                      className="text-sm font-semibold text-slate-900 hover:text-slate-600"
                    >
                      View job →
                    </Link>
                  </div>
                </div>
              ))}
            </div>
            {error && (
              <p className="mt-4 text-xs text-rose-600">{error}</p>
            )}
          </div>

          <div className="flex flex-col gap-6">
            <div className="flex flex-col gap-4 rounded-2xl border border-dashed border-slate-300 bg-slate-100 p-6">
              <h3 className="text-lg font-semibold text-slate-900">
                How it works
              </h3>
              <ol className="space-y-3 text-sm text-slate-600">
                <li>1. Connect a file, API, or cloud bucket.</li>
                <li>2. We profile fields and flag issues.</li>
                <li>3. Define the target schema mapping.</li>
                <li>4. Run the ingestion and monitor results.</li>
              </ol>
              <Link
                href="/app/jobs/new"
                className="mt-auto inline-flex items-center justify-center rounded-full border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-900 hover:bg-slate-50"
              >
                Add data source
              </Link>
            </div>
          </div>
        </div>
      </section>
    </AppShell>
  );
}
