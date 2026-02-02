"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AppShell from "@/components/AppShell";
import { createSchema, fetchSchema } from "@/lib/api";
import { parseMappingSpec } from "@/lib/mapping";

const defaultSchema = "{}";

export default function NewSchemaClient() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const cloneId = searchParams.get("clone");

  const [name, setName] = useState("");
  const [schemaText, setSchemaText] = useState(defaultSchema);
  const [mappingText, setMappingText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [apiKey, setApiKey] = useState<string | null>(null);

  useEffect(() => {
    const loadClone = async () => {
      if (!cloneId) {
        return;
      }
      try {
        const response = await fetchSchema(cloneId);
        const schema = response.schema;
        setName(`Copy of ${schema.name}`);
        setSchemaText(JSON.stringify(schema.schemaDefinition, null, 2));
        if (schema.defaultMapping) {
          setMappingText(JSON.stringify(schema.defaultMapping, null, 2));
        }
      } catch {
        setError("Failed to load schema to clone.");
      }
    };
    loadClone();
  }, [cloneId]);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoading(true);
    setError(null);
    setApiKey(null);

    let schemaDefinition: unknown;
    try {
      schemaDefinition = JSON.parse(schemaText);
    } catch {
      setError("Schema definition must be valid JSON.");
      setLoading(false);
      return;
    }

    const mapping = mappingText.trim() ? parseMappingSpec(mappingText) : null;
    if (mappingText.trim() && !mapping) {
      setError("Default mapping must be valid JSON with a mappings array.");
      setLoading(false);
      return;
    }

    try {
      const response = await createSchema({
        name,
        schemaDefinition,
        defaultMapping: mapping ?? undefined,
      });
      setApiKey(response.apiKey ?? null);
    } catch {
      setError("Failed to deploy schema.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <AppShell>
      <div className="flex flex-col gap-6">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              New schema
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              Deploy a target schema
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              Define the target structure and optional default mapping.
            </p>
          </div>
          <Link
            href="/app/schemas"
            className="text-sm font-semibold text-slate-600 hover:text-slate-900"
          >
            ← Back to schemas
          </Link>
        </div>

        <form
          onSubmit={handleSubmit}
          className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr]"
        >
          <section className="flex flex-col gap-6 rounded-2xl bg-white p-6 shadow-sm">
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Schema name
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
                value={name}
                onChange={(event) => setName(event.target.value)}
                required
              />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Schema definition (JSON)
              </label>
              <textarea
                className="mt-2 min-h-[200px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
                value={schemaText}
                onChange={(event) => setSchemaText(event.target.value)}
              />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                Default mapping (optional)
              </label>
              <textarea
                className="mt-2 min-h-[180px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
                value={mappingText}
                onChange={(event) => setMappingText(event.target.value)}
              />
            </div>
          </section>

          <aside className="flex flex-col gap-4 rounded-2xl bg-white p-6 shadow-sm">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              <p className="text-sm font-semibold text-slate-900">
                Schema deployment tips
              </p>
              <ul className="mt-3 space-y-2">
                <li>Keep field names stable for reuse.</li>
                <li>Default mappings speed up ingestion runs.</li>
                <li>Versioning happens on every update.</li>
              </ul>
            </div>
            {error && (
              <p className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                {error}
              </p>
            )}
            {apiKey ? (
              <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-xs text-emerald-900">
                <p className="font-semibold">API key generated</p>
                <p className="mt-2 break-all font-mono text-[11px]">{apiKey}</p>
                <div className="mt-3 flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => navigator.clipboard.writeText(apiKey)}
                    className="rounded-full bg-emerald-600 px-3 py-1 text-xs font-semibold text-white hover:bg-emerald-500"
                  >
                    Copy key
                  </button>
                  <button
                    type="button"
                    onClick={() => router.push("/app/schemas")}
                    className="rounded-full border border-emerald-200 px-3 py-1 text-xs font-semibold text-emerald-800"
                  >
                    Back to schemas
                  </button>
                </div>
              </div>
            ) : (
              <button
                type="submit"
                disabled={loading}
                className="rounded-full bg-slate-900 px-4 py-3 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
              >
                {loading ? "Deploying..." : "Deploy schema"}
              </button>
            )}
          </aside>
        </form>
      </div>
    </AppShell>
  );
}
