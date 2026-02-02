"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { listSchemas, SchemaSummary } from "@/lib/api";

export default function Home() {
  const [schemas, setSchemas] = useState<SchemaSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const response = await listSchemas();
        setSchemas(response.schemas ?? []);
        setError(null);
      } catch {
        setSchemas([]);
        setError("Unable to load mappings right now.");
      } finally {
        setLoading(false);
      }
    };
    loadData();
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
                Map any input to your target schema.
              </h1>
              <p className="mt-3 max-w-2xl text-base text-slate-600">
                Create a mapping once, then reuse it to normalize every input.
              </p>
            </div>
            <Link
              href="/app/schemas/new"
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
            >
              Create mapping
            </Link>
          </div>
        </div>

        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-slate-900">
              Mappings
            </h2>
            <span className="text-sm text-slate-500">
              {schemas.length} total
            </span>
          </div>
          <div className="mt-6 divide-y divide-slate-200">
            {loading && (
              <div className="py-6 text-sm text-slate-500">
                Loading mappings...
              </div>
            )}
            {!loading && schemas.length === 0 && (
              <div className="py-6 text-sm text-slate-500">
                No mappings yet.
              </div>
            )}
            {schemas.filter((schema) => schema.id).map((schema) => {
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
                      Version {schema.version} • Updated {schema.updatedAt}
                    </p>
                  </div>
                  <div className="flex items-center gap-3">
                    <Link
                      href={`/app/schemas/${schema.id}`}
                      className="text-sm font-semibold text-slate-900 hover:text-slate-600"
                    >
                      Open →
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
