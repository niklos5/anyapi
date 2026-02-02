"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AppShell from "@/components/AppShell";
import { analyzeSource, ingestSchema, listSchemas, SchemaSummary } from "@/lib/api";
import { parseMappingSpec } from "@/lib/mapping";

export default function JobNewClient() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [sourceType, setSourceType] = useState("file");
  const [mappingText, setMappingText] = useState("");
  const [schemas, setSchemas] = useState<SchemaSummary[]>([]);
  const [selectedSchemaId, setSelectedSchemaId] = useState<string>("");
  const [loadingSchemas, setLoadingSchemas] = useState(true);
  const [sourcePayloadText, setSourcePayloadText] = useState("");
  const [fileName, setFileName] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [analysis, setAnalysis] = useState<{
    schema: Record<string, string>;
    preview: Record<string, unknown>[];
    issues: { field: string; level: string; message: string }[];
  } | null>(null);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadSchemas = async () => {
      try {
        const response = await listSchemas();
        const items = response.schemas ?? [];
        setSchemas(items);
        if (items.length > 0) {
          setSelectedSchemaId(items[0].id);
        }
      } catch {
        setSchemas([]);
      } finally {
        setLoadingSchemas(false);
      }
    };
    loadSchemas();
  }, []);

  useEffect(() => {
    const schemaParam = searchParams.get("schema");
    if (!schemaParam || schemas.length === 0) {
      return;
    }
    const exists = schemas.some((schema) => schema.id === schemaParam);
    if (exists) {
      setSelectedSchemaId(schemaParam);
    }
  }, [schemas, searchParams]);

  const selectedSchema = useMemo(
    () => schemas.find((schema) => schema.id === selectedSchemaId) ?? null,
    [schemas, selectedSchemaId]
  );

  useEffect(() => {
    if (selectedSchema?.defaultMapping) {
      setMappingText(JSON.stringify(selectedSchema.defaultMapping, null, 2));
    }
  }, [selectedSchema]);

  const parseSourcePayload = () => {
    if (!sourcePayloadText.trim()) {
      setError("Paste a sample JSON payload before continuing.");
      return null;
    }
    try {
      return JSON.parse(sourcePayloadText);
    } catch {
      setError("Source payload must be valid JSON.");
      return null;
    }
  };

  const handleFile = async (file: File) => {
    setError(null);
    setAnalysis(null);
    setFileName(file.name);
    const text = await file.text();
    const isJson =
      file.type === "application/json" ||
      file.name.toLowerCase().endsWith(".json");
    if (!isJson) {
      setError("Only JSON files are supported right now.");
      return;
    }
    try {
      const parsed = JSON.parse(text);
      setSourcePayloadText(JSON.stringify(parsed, null, 2));
    } catch {
      setError("Uploaded JSON file is invalid.");
    }
  };

  const handleFileChange = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0];
    if (file) {
      await handleFile(file);
    }
  };

  const handleDrop = async (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragging(false);
    const file = event.dataTransfer.files?.[0];
    if (file) {
      await handleFile(file);
    }
  };

  const handleAnalyze = async () => {
    setLoadingAnalysis(true);
    setError(null);
    try {
      const payload = parseSourcePayload();
      if (!payload) {
        setLoadingAnalysis(false);
        return;
      }
      const response = await analyzeSource(payload);
      setAnalysis(response);
    } catch (err) {
      setError("Failed to analyze data source.");
    } finally {
      setLoadingAnalysis(false);
    }
  };

  const handleRun = async () => {
    setSubmitting(true);
    setError(null);
    if (!selectedSchemaId) {
      setError("Select a target schema before running ingestion.");
      setSubmitting(false);
      return;
    }
    const parsed = mappingText.trim()
      ? parseMappingSpec(mappingText)
      : selectedSchema?.defaultMapping ?? null;
    if (!parsed) {
      setError("Mapping spec must be valid JSON with a mappings array.");
      setSubmitting(false);
      return;
    }
    try {
      const payload = parseSourcePayload();
      if (!payload) {
        setSubmitting(false);
        return;
      }
      const response = await ingestSchema(selectedSchemaId, {
        name: "Ingestion job",
        sourceType,
        data: payload,
        mapping: parsed,
      });
      router.push(`/app/jobs/${response.job.id}`);
    } catch (err) {
      setError("Failed to create ingestion job.");
    } finally {
      setSubmitting(false);
    }
  };

  const previewRows = analysis?.preview ?? [];

  return (
    <AppShell>
      <div className="flex flex-col gap-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
              New ingestion
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              Create a new ingestion job
            </h1>
            <p className="mt-2 text-base text-slate-600">
              Connect a data source, review the automatic analysis, and map it
              to your target schema.
            </p>
          </div>
          <Link
            href="/app"
            className="text-sm font-semibold text-slate-600 hover:text-slate-900"
          >
            ← Back to dashboard
          </Link>
        </div>

        <div className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
          <section className="flex flex-col gap-6 rounded-2xl bg-white p-6 shadow-sm">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                0. Select a target schema
              </h2>
              <p className="text-sm text-slate-500">
                Choose a deployed schema or create a new one.
              </p>
              <div className="mt-4 flex flex-wrap items-center gap-3">
                <select
                  className="min-w-[220px] rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700"
                  value={selectedSchemaId}
                  onChange={(event) => setSelectedSchemaId(event.target.value)}
                  disabled={loadingSchemas || schemas.length === 0}
                >
                  {schemas.length === 0 && (
                    <option value="">No schemas deployed</option>
                  )}
                  {schemas.map((schema) => (
                    <option key={schema.id} value={schema.id}>
                      {schema.name} (v{schema.version})
                    </option>
                  ))}
                </select>
                <Link
                  href="/app/schemas/new"
                  className="text-sm font-semibold text-slate-600 hover:text-slate-900"
                >
                  + Deploy new schema
                </Link>
              </div>
              {selectedSchema && (
                <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
                  <p className="font-semibold text-slate-900">
                    {selectedSchema.name}
                  </p>
                  <p className="mt-1">
                    Updated {selectedSchema.updatedAt} • Version{" "}
                    {selectedSchema.version}
                  </p>
                </div>
              )}
            </div>
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                1. Connect a data source
              </h2>
              <p className="text-sm text-slate-500">
                Choose a source and provide credentials or a file.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              {[
                { id: "file", label: "File upload (CSV/JSON)" },
                { id: "api", label: "External API" },
                { id: "cloud", label: "Cloud storage" },
              ].map((option) => (
                <button
                  key={option.id}
                  type="button"
                  onClick={() => setSourceType(option.id)}
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                    sourceType === option.id
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-slate-200 bg-white text-slate-600 hover:border-slate-400"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <div className="grid gap-4 rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
              {sourceType === "file" && (
                <>
                  <p className="font-semibold text-slate-900">
                    Upload a CSV or JSON file
                  </p>
                  <div
                    className={`flex flex-col gap-2 rounded-lg border border-dashed px-4 py-3 transition ${
                      isDragging
                        ? "border-slate-900 bg-slate-50"
                        : "border-slate-300 bg-white"
                    }`}
                    onDragEnter={(event) => {
                      event.preventDefault();
                      setIsDragging(true);
                    }}
                    onDragOver={(event) => event.preventDefault()}
                    onDragLeave={() => setIsDragging(false)}
                    onDrop={handleDrop}
                  >
                    <div className="flex items-center justify-between">
                      <span>
                        {fileName
                          ? `Selected: ${fileName}`
                          : "Drop file here or browse"}
                      </span>
                      <label className="cursor-pointer rounded-full bg-slate-900 px-3 py-1 text-xs font-semibold text-white">
                        Upload
                        <input
                          type="file"
                          accept=".json,application/json"
                          className="hidden"
                          onChange={handleFileChange}
                        />
                      </label>
                    </div>
                    <p className="text-xs text-slate-500">
                      JSON files only for now. Paste JSON below if preferred.
                    </p>
                  </div>
                </>
              )}
              {sourceType === "api" && (
                <>
                  <p className="font-semibold text-slate-900">
                    Connect an external API
                  </p>
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="https://api.example.com/orders"
                  />
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="API key (read-only)"
                  />
                </>
              )}
              {sourceType === "cloud" && (
                <>
                  <p className="font-semibold text-slate-900">
                    Connect cloud storage
                  </p>
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="s3://bucket-name/ingest/"
                  />
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="IAM access token"
                  />
                </>
              )}
            </div>

            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                2. Automatic analysis
              </h2>
              <p className="text-sm text-slate-500">
                We inspect the data to detect fields and potential issues.
              </p>
              <textarea
                className="mt-4 min-h-[140px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
                placeholder="Paste a sample JSON payload here (array or object)."
                value={sourcePayloadText}
                onChange={(event) => setSourcePayloadText(event.target.value)}
              />
              <button
                type="button"
                onClick={handleAnalyze}
                disabled={loadingAnalysis || !sourcePayloadText.trim()}
                className="mt-4 inline-flex items-center justify-center rounded-full border border-slate-300 bg-white px-4 py-2 text-xs font-semibold text-slate-900 hover:bg-slate-50"
              >
                {loadingAnalysis ? "Analyzing..." : "Analyze data"}
              </button>
              <div className="mt-4 grid gap-4 md:grid-cols-3">
                {[
                  {
                    label: "Fields detected",
                    value: analysis ? Object.keys(analysis.schema).length : "—",
                  },
                  {
                    label: "Records scanned",
                    value: analysis ? analysis.preview.length : "—",
                  },
                  {
                    label: "Potential issues",
                    value: analysis ? analysis.issues.length : "—",
                  },
                ].map((metric) => (
                  <div
                    key={metric.label}
                    className="rounded-xl border border-slate-200 bg-white p-4"
                  >
                    <p className="text-xs uppercase tracking-wide text-slate-500">
                      {metric.label}
                    </p>
                    <p className="mt-2 text-xl font-semibold text-slate-900">
                      {metric.value}
                    </p>
                  </div>
                ))}
              </div>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                3. Map to target schema
              </h2>
              <p className="text-sm text-slate-500">
                Provide the target schema and mapping rules.
              </p>
              <textarea
                className="mt-3 min-h-[180px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
                value={mappingText}
                onChange={(event) => setMappingText(event.target.value)}
              />
            </div>
          </section>

          <aside className="flex flex-col gap-6 rounded-2xl bg-white p-6 shadow-sm">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                Data preview
              </h2>
              <p className="text-sm text-slate-500">
                Sample rows after processing.
              </p>
            </div>
            {previewRows.length === 0 ? (
              <p className="text-sm text-slate-500">
                Run analysis to preview mapped rows.
              </p>
            ) : (
              <div className="overflow-hidden rounded-xl border border-slate-200">
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
            <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-xs text-amber-800">
              <p className="font-semibold">Detected issues</p>
              <ul className="mt-2 space-y-1">
                {(analysis?.issues ?? []).length === 0 && (
                  <li>• No issues detected.</li>
                )}
                {(analysis?.issues ?? []).map((issue) => (
                  <li key={`${issue.field}-${issue.message}`}>
                    • {issue.field}: {issue.message}
                  </li>
                ))}
              </ul>
            </div>
            {error && (
              <p className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                {error}
              </p>
            )}
            <button
              onClick={handleRun}
              disabled={submitting}
              className="mt-auto rounded-full bg-slate-900 px-4 py-3 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
            >
              {submitting ? "Running..." : "Confirm and run ingestion"}
            </button>
          </aside>
        </div>
      </div>
    </AppShell>
  );
}
