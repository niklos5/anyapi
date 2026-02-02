"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import StatusBadge from "@/components/StatusBadge";
import { fetchJob, JobSummary } from "@/lib/api";

type JobDetailPageProps = {
  params: { jobId: string };
};

export default function JobDetailPage({ params }: JobDetailPageProps) {
  const [job, setJob] = useState<JobSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadJob = async () => {
      try {
        const response = await fetchJob(params.jobId);
        setJob(response);
        setError(null);
      } catch {
        setJob(null);
        setError("Unable to load job details.");
      }
    };
    loadJob();
  }, [params.jobId]);

  if (!job) {
    return (
      <AppShell>
        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <p className="text-sm text-slate-500">
            {error ?? "Loading job..."}
          </p>
        </div>
      </AppShell>
    );
  }

  const recordsCount = (() => {
    const value = (job as unknown as { records?: unknown }).records;
    return typeof value === "number" ? value : null;
  })();
  const issuesList = (() => {
    const value = (job as unknown as { issues?: unknown }).issues;
    return Array.isArray(value)
      ? (value as { field: string; message: string }[])
      : [];
  })();

  return (
    <AppShell>
      <div className="flex flex-col gap-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              Job detail
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              {job.name}
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              {job.sourceType.toUpperCase()} source • Created {job.createdAt}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status={job.status} />
            <Link
              href="/app"
              className="text-sm font-semibold text-slate-600 hover:text-slate-900"
            >
              Back to dashboard
            </Link>
          </div>
        </div>

        <div className="grid gap-6 lg:grid-cols-[1.4fr_0.6fr]">
          <section className="rounded-2xl bg-white p-6 shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">
              Job overview
            </h2>
            <p className="mt-2 text-sm text-slate-600">
              Track processing status and metrics once data lands.
            </p>
          </section>

          <aside className="flex flex-col gap-6 rounded-2xl bg-white p-6 shadow-sm">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                Job metrics
              </h2>
              <div className="mt-4 grid gap-3 text-sm text-slate-600">
                <div className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2">
                  <span>Records processed</span>
                  <span className="font-semibold text-slate-900">
                    {recordsCount !== null
                      ? recordsCount.toLocaleString()
                      : "—"}
                  </span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2">
                  <span>Issues flagged</span>
                  <span className="font-semibold text-slate-900">
                    {issuesList.length}
                  </span>
                </div>
              </div>
            </div>
            {issuesList.length > 0 && (
              <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-xs text-rose-800">
                <p className="font-semibold">Errors & warnings</p>
                <ul className="mt-2 space-y-1">
                  {issuesList.map((issue) => (
                    <li key={issue.field}>
                      • {issue.field}: {issue.message}
                    </li>
                  ))}
                </ul>
              </div>
            )}
            <Link
              href={`/app/jobs/${job.id}/results`}
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-4 py-3 text-sm font-semibold text-white hover:bg-slate-800"
            >
              View results
            </Link>
          </aside>
        </div>
      </div>
    </AppShell>
  );
}
