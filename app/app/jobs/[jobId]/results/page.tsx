"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { fetchJobResults } from "@/lib/api";
import { mockPreviewRows } from "@/lib/mock";

type JobResultsPageProps = {
  params: { jobId: string };
};

export default function JobResultsPage({ params }: JobResultsPageProps) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [usingMock, setUsingMock] = useState(false);

  useEffect(() => {
    const loadResults = async () => {
      try {
        const response = await fetchJobResults(params.jobId);
        const result = response.result ?? {};
        if (Array.isArray(result.items)) {
          setRows(result.items);
        } else if (Array.isArray(result)) {
          setRows(result);
        } else {
          setRows([result]);
        }
        setUsingMock(false);
      } catch {
        setRows(mockPreviewRows);
        setUsingMock(true);
      }
    };
    loadResults();
  }, [params.jobId]);

  const previewRows = rows.length > 0 ? rows : mockPreviewRows;
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
              Job ID: {params.jobId} • Redwood Supply Co.
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
            <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-xs text-rose-800">
              <p className="font-semibold">2 issues found</p>
              <ul className="mt-2 space-y-1">
                <li>• customer_phone: 42 records missing phone number.</li>
                <li>• order_date: Mixed date formats detected.</li>
              </ul>
            </div>
            <button className="rounded-full border border-slate-300 bg-white px-4 py-3 text-sm font-semibold text-slate-900 hover:bg-slate-50">
              Retry ingestion
            </button>
            {usingMock && (
              <p className="text-xs text-slate-500">
                Backend not reachable, showing sample results.
              </p>
            )}

            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              <p className="text-sm font-semibold text-slate-900">
                Customer success notes
              </p>
              <ul className="mt-3 space-y-2">
                <li>Weekly SLA report scheduled for Fridays.</li>
                <li>Mapped fields align with finance reconciliations.</li>
                <li>Alerting routed to Data Ops on-call.</li>
              </ul>
            </div>
          </aside>
        </div>
      </div>
    </AppShell>
  );
}
