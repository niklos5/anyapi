"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import AppShell from "@/components/AppShell";
import { listSchemas, SchemaSummary } from "@/lib/api";

export default function SchemasPage() {
  const [schemas, setSchemas] = useState<SchemaSummary[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadSchemas = async () => {
      try {
        const response = await listSchemas();
        setSchemas(response.schemas ?? []);
      } catch {
        setSchemas([]);
      } finally {
        setLoading(false);
      }
    };
    loadSchemas();
  }, []);

  return (
    <AppShell>
      <div className="flex flex-col gap-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              Mappings
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              All mappings
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              Each mapping turns any input into your target schema.
            </p>
          </div>
          <Link
            href="/app/schemas/new"
            className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800"
          >
            Create mapping
          </Link>
        </div>

        <div className="rounded-2xl bg-white p-6 shadow-sm">
          {loading && (
            <p className="text-sm text-slate-500">Loading mappings...</p>
          )}
          {!loading && schemas.length === 0 && (
            <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-6 text-sm text-slate-600">
              No mappings yet. Create a mapping to reuse it across inputs.
            </div>
          )}
          {!loading && schemas.length > 0 && (
            <div className="divide-y divide-slate-200">
              {schemas.map((schema) => (
                <div
                  key={schema.id}
                  className="flex flex-col gap-3 py-4 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div>
                    <p className="text-sm font-semibold text-slate-900">
                      {schema.name}
                    </p>
                    <p className="text-xs text-slate-500">
                      Version {schema.version} • Updated {schema.updatedAt}
                    </p>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-slate-500">
                    <span>
                      Target schema:{" "}
                      {schema.schemaDefinition ? "Configured" : "Missing"}
                    </span>
                    <Link
                      href={`/app/schemas/${schema.id}`}
                      className="text-sm font-semibold text-slate-700 hover:text-slate-900"
                    >
                      Open
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
