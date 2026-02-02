"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { fetchJobResults } from "@/lib/api";

type JobResultsPageProps = {
  params: { jobId: string };
};

export default function JobResultsPage({ params }: JobResultsPageProps) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadResults = async () => {
      try {
        const response = await fetchJobResults(params.jobId);
        const result = response.result ?? {};
        if (
          result &&
          typeof result === "object" &&
          "items" in result &&
          Array.isArray((result as { items?: unknown }).items)
        ) {
          setRows((result as { items: Record<string, unknown>[] }).items);
        } else if (Array.isArray(result)) {
          setRows(result);
        } else if (result && typeof result === "object") {
          setRows([result as Record<string, unknown>]);
        } else {
          setRows([]);
        }
        setError(null);
      } catch {
        setRows([]);
        setError("Unable to load results right now.");
      }
    };
    loadResults();
  }, [params.jobId]);

  const previewRows = rows;
  return (
    <AppShell>
      <div className="flex flex-col gap-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              Results
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              Processed data output
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              Job ID: {params.jobId}
            </p>
          </div>
          <Link
            href={`/app/jobs/${params.jobId}`}
            className="text-sm font-semibold text-slate-600 hover:text-slate-900"
          >
            ← Back to job
          </Link>
        </div>

        <div className="grid gap-6 lg:grid-cols-[1.4fr_0.6fr]">
          <section className="rounded-2xl bg-white p-6 shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">
              Final mapped data
            </h2>
            {previewRows.length === 0 ? (
              <p className="mt-4 text-sm text-slate-500">
                No results available yet.
              </p>
            ) : (
              <div className="mt-4 overflow-hidden rounded-xl border border-slate-200">
                <table className="w-full text-left text-xs text-slate-600">
                  <thead className="bg-slate-100 text-[11px] uppercase tracking-wide text-slate-500">
                    <tr>
                      {Object.keys(
                        (previewRows[0] as Record<string, unknown>) ?? {}
                      ).map((key) => (
                        <th key={key} className="px-3 py-2">
                          {key}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewRows.map((row, index) => (
                      <tr key={`row-${index}`} className="border-t">
                        {Object.values(row as Record<string, unknown>).map(
                          (value, valueIndex) => (
                            <td
                              key={`${index}-${valueIndex}`}
                              className="px-3 py-2"
                            >
                              {String(value)}
                            </td>
                          )
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <aside className="flex flex-col gap-6 rounded-2xl bg-white p-6 shadow-sm">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                Errors & warnings
              </h2>
              <p className="text-sm text-slate-500">
                Items flagged during processing.
              </p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              <p className="font-semibold text-slate-900">No issues reported</p>
              <p className="mt-2">
                Issues and warnings will appear here after processing.
              </p>
            </div>
            <button className="rounded-full border border-slate-300 bg-white px-4 py-3 text-sm font-semibold text-slate-900 hover:bg-slate-50">
              Retry ingestion
            </button>
            {error && (
              <p className="text-xs text-rose-600">{error}</p>
            )}
          </aside>
        </div>
      </div>
    </AppShell>
  );
}
