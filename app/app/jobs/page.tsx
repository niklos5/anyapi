"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import StatusBadge from "@/components/StatusBadge";
import { listJobs, JobSummary } from "@/lib/api";

export default function JobsPage() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadJobs = async () => {
      try {
        const response = await listJobs();
        setJobs(response.jobs ?? []);
      } catch {
        setJobs([]);
      } finally {
        setLoading(false);
      }
    };
    loadJobs();
  }, []);

  return (
    <AppShell>
      <div className="flex flex-col gap-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              Jobs
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              Ingestion jobs
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              Track queued, processing, and completed runs.
            </p>
          </div>
          <Link
            href="/app/jobs/new"
            className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
          >
            New ingestion
          </Link>
        </div>

        <div className="rounded-2xl bg-white p-6 shadow-sm">
          {loading && (
            <p className="text-sm text-slate-500">Loading jobs...</p>
          )}
          {!loading && jobs.length === 0 && (
            <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-6 text-sm text-slate-600">
              No ingestion jobs yet. Run a mapping to create one.
            </div>
          )}
          {!loading && jobs.length > 0 && (
            <div className="divide-y divide-slate-200">
              {jobs.map((job) => (
                <div
                  key={job.id}
                  className="flex flex-col gap-3 py-4 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div>
                    <p className="text-sm font-semibold text-slate-900">
                      {job.name}
                    </p>
                    <p className="text-xs text-slate-500">
                      {job.sourceType.toUpperCase()} • Created {job.createdAt}
                    </p>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-slate-500">
                    <StatusBadge status={job.status} />
                    {job.schemaId && (
                      <Link
                        href={`/app/schemas/${job.schemaId}`}
                        className="text-sm font-semibold text-slate-700 hover:text-slate-900"
                      >
                        View mapping
                      </Link>
                    )}
                    <Link
                      href={`/app/jobs/${job.id}`}
                      className="text-sm font-semibold text-slate-700 hover:text-slate-900"
                    >
                      View job
                    </Link>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </AppShell>
  );
}
