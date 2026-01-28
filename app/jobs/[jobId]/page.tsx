"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import StatusBadge from "@/components/StatusBadge";
import { fetchJob, JobSummary } from "@/lib/api";
import { mockJobs } from "@/lib/mock";

type JobDetailPageProps = {
  params: { jobId: string };
};

export default function JobDetailPage({ params }: JobDetailPageProps) {
  const [job, setJob] = useState<JobSummary | null>(null);
  const [usingMock, setUsingMock] = useState(false);

  useEffect(() => {
    const loadJob = async () => {
      try {
        const response = await fetchJob(params.jobId);
        setJob(response);
        setUsingMock(false);
      } catch {
        const fallback =
          mockJobs.find((item) => item.id === params.jobId) ?? mockJobs[0];
        setJob({
          id: fallback.id,
          name: fallback.name,
          sourceType: fallback.sourceType,
          status: fallback.status,
          createdAt: fallback.createdAt,
        });
        setUsingMock(true);
      }
    };
    loadJob();
  }, [params.jobId]);

  if (!job) {
    return (
      <AppShell>
        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <p className="text-sm text-slate-500">Loading job...</p>
        </div>
      </AppShell>
    );
  }

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
              {job.sourceType.toUpperCase()} source • Created {job.createdAt} •
              Redwood Supply Co.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status={job.status} />
            <Link
              href="/"
              className="text-sm font-semibold text-slate-600 hover:text-slate-900"
            >
              Back to dashboard
            </Link>
          </div>
        </div>

        <div className="grid gap-6 lg:grid-cols-[1.4fr_0.6fr]">
          <section className="flex flex-col gap-6">
            <div className="rounded-2xl bg-white p-6 shadow-sm">
              <h2 className="text-lg font-semibold text-slate-900">
                Company profile
              </h2>
              <div className="mt-4 grid gap-3 text-sm text-slate-600 sm:grid-cols-2">
                <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <p className="text-xs uppercase tracking-wide text-slate-500">
                    Business unit
                  </p>
                  <p className="font-semibold text-slate-900">
                    Omni-channel Fulfillment
                  </p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <p className="text-xs uppercase tracking-wide text-slate-500">
                    SLA target
                  </p>
                  <p className="font-semibold text-slate-900">15 min</p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <p className="text-xs uppercase tracking-wide text-slate-500">
                    Owner
                  </p>
                  <p className="font-semibold text-slate-900">
                    Riley Chen, Data Ops
                  </p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <p className="text-xs uppercase tracking-wide text-slate-500">
                    Schema
                  </p>
                  <p className="font-semibold text-slate-900">
                    UnifiedOrder v2
                  </p>
                </div>
              </div>
            </div>

            <div className="rounded-2xl bg-white p-6 shadow-sm">
              <h2 className="text-lg font-semibold text-slate-900">
                Job timeline
              </h2>
              <div className="mt-6 space-y-4 text-sm text-slate-600">
                {[
                  "Source connected and validated.",
                  "Data profiling completed.",
                  "Schema mapping applied.",
                  "Ingestion processing started.",
                  "Results generated and stored.",
                ].map((item, index) => (
                  <div key={item} className="flex items-start gap-3">
                    <div className="mt-1 h-2 w-2 rounded-full bg-slate-900" />
                    <div>
                      <p className="font-medium text-slate-900">
                        Step {index + 1}
                      </p>
                      <p>{item}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
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
                    {"records" in job ? job.records.toLocaleString() : "—"}
                  </span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2">
                  <span>Issues flagged</span>
                  <span className="font-semibold text-slate-900">
                    {"issues" in job ? job.issues.length : "—"}
                  </span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2">
                  <span>Target schema</span>
                  <span className="font-semibold text-slate-900">
                    UnifiedOrder
                  </span>
                </div>
              </div>
            </div>
            {"issues" in job && job.issues.length > 0 && (
              <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-xs text-rose-800">
                <p className="font-semibold">Errors & warnings</p>
                <ul className="mt-2 space-y-1">
                  {job.issues.map((issue) => (
                    <li key={issue.field}>
                      • {issue.field}: {issue.message}
                    </li>
                  ))}
                </ul>
              </div>
            )}
            <Link
              href={`/jobs/${job.id}/results`}
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-4 py-3 text-sm font-semibold text-white hover:bg-slate-800"
            >
              View results
            </Link>
            {usingMock && (
              <p className="text-xs text-slate-500">
                Backend not reachable, showing sample job.
              </p>
            )}
          </aside>
        </div>
      </div>
    </AppShell>
  );
}
