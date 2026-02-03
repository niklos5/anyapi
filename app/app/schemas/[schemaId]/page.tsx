"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import AppShell from "@/components/AppShell";
import {
  analyzeSource,
  fetchSchema,
  ingestSchema,
  listJobs,
  updateSchema,
  JobSummary,
  SchemaSummary,
} from "@/lib/api";
import { parseMappingSpec } from "@/lib/mapping";
import {
  TransformerInputConfig,
  TransformerMetadata,
  TransformerOutputConfig,
  TransformerSettings,
} from "@/lib/types";

const defaultInputConfig: TransformerInputConfig = {
  sourceType: "file",
};

const defaultOutputConfig: TransformerOutputConfig = {
  destinationType: "webhook",
};

const defaultSettings: TransformerSettings = {
  dedupeEnabled: false,
};

const parseJson = (value: string) => {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

export default function SchemaDetailPage() {
  const params = useParams();
  const schemaId =
    typeof params?.schemaId === "string" ? params.schemaId : undefined;
  const [schema, setSchema] = useState<SchemaSummary | null>(null);
  const [name, setName] = useState("");
  const [schemaText, setSchemaText] = useState("{}");
  const [mappingText, setMappingText] = useState("");
  const [inputConfig, setInputConfig] =
    useState<TransformerInputConfig>(defaultInputConfig);
  const [outputConfig, setOutputConfig] =
    useState<TransformerOutputConfig>(defaultOutputConfig);
  const [settings, setSettings] =
    useState<TransformerSettings>(defaultSettings);
  const [sourcePayloadText, setSourcePayloadText] = useState("");
  const [analysis, setAnalysis] = useState<{
    schema: Record<string, string>;
    preview: Record<string, unknown>[];
    issues: { field: string; level: string; message: string }[];
  } | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [savingSchema, setSavingSchema] = useState(false);
  const [savingConfig, setSavingConfig] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [runJobId, setRunJobId] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [jobsLoading, setJobsLoading] = useState(false);

  useEffect(() => {
    const loadSchema = async () => {
      if (
        !schemaId ||
        schemaId === "undefined" ||
        schemaId === "null" ||
        schemaId === "None"
      ) {
        setSchema(null);
        setError("Missing mapping ID.");
        setLoading(false);
        return;
      }
      try {
        const response = await fetchSchema(schemaId);
        const record = response.schema;
        setSchema(record);
        setName(record.name);
        setSchemaText(JSON.stringify(record.schemaDefinition ?? {}, null, 2));
        if (record.defaultMapping) {
          setMappingText(JSON.stringify(record.defaultMapping, null, 2));
        }
        const metadata: TransformerMetadata = record.metadata ?? {};
        setInputConfig(metadata.input ?? defaultInputConfig);
        setOutputConfig(metadata.output ?? defaultOutputConfig);
        setSettings(metadata.settings ?? defaultSettings);
        setError(null);
      } catch {
        setSchema(null);
        setError("Unable to load mapping.");
      } finally {
        setLoading(false);
      }
    };
    loadSchema();
  }, [schemaId]);

  const loadJobs = async () => {
    if (!schemaId) {
      return;
    }
    setJobsLoading(true);
    try {
      const response = await listJobs();
      const filtered = (response.jobs ?? []).filter(
        (job) => job.schemaId === schemaId
      );
      setJobs(filtered);
    } catch {
      setJobs([]);
    } finally {
      setJobsLoading(false);
    }
  };

  useEffect(() => {
    loadJobs();
  }, [schemaId]);

  const metrics = useMemo(() => {
    if (!analysis) {
      return { fields: "—", rows: "—", issues: "—" };
    }
    return {
      fields: Object.keys(analysis.schema ?? {}).length,
      rows: analysis.preview.length,
      issues: analysis.issues.length,
    };
  }, [analysis]);

  const handleAnalyze = async () => {
    setStatusMessage(null);
    setError(null);
    const payload = parseJson(sourcePayloadText.trim());
    if (!payload) {
      setError("Paste a valid JSON payload to analyze.");
      return;
    }
    try {
      const response = await analyzeSource(payload);
      setAnalysis(response);
    } catch {
      setError("Unable to analyze the input.");
    }
  };

  const handleFile = async (file: File) => {
    setError(null);
    setAnalysis(null);
    setFileName(file.name);
    const text = await file.text();
    try {
      const parsed = JSON.parse(text);
      setSourcePayloadText(JSON.stringify(parsed, null, 2));
    } catch {
      setError("Uploaded JSON file is invalid.");
    }
  };

  const handleSaveSchema = async () => {
    setSavingSchema(true);
    setError(null);
    setStatusMessage(null);
    if (!schemaId) {
      setError("Missing mapping ID.");
      setSavingSchema(false);
      return;
    }
    const schemaDefinition = parseJson(schemaText);
    if (!schemaDefinition) {
      setError("Target schema must be valid JSON.");
      setSavingSchema(false);
      return;
    }
    const trimmedMapping = mappingText.trim();
    const parsedMapping = trimmedMapping ? parseMappingSpec(trimmedMapping) : null;
    if (trimmedMapping && !parsedMapping) {
      setError("Mapping rules must be valid JSON.");
      setSavingSchema(false);
      return;
    }
    try {
      const response = await updateSchema(schemaId, {
        name,
        schemaDefinition,
        defaultMapping: parsedMapping ?? undefined,
      });
      setSchema(response.schema);
      setStatusMessage("Schema updated.");
    } catch {
      setError("Unable to update schema.");
    } finally {
      setSavingSchema(false);
    }
  };

  const handleSaveConfig = async () => {
    setSavingConfig(true);
    setError(null);
    setStatusMessage(null);
    if (!schemaId) {
      setError("Missing mapping ID.");
      setSavingConfig(false);
      return;
    }
    try {
      const response = await updateSchema(schemaId, {
        metadata: {
          input: inputConfig,
          output: outputConfig,
          settings,
        },
      });
      setSchema(response.schema);
      setStatusMessage("Configuration saved.");
    } catch {
      setError("Unable to save configuration.");
    } finally {
      setSavingConfig(false);
    }
  };

  const handleRun = async () => {
    setRunning(true);
    setError(null);
    setStatusMessage(null);
    if (!schemaId) {
      setError("Missing mapping ID.");
      setRunning(false);
      return;
    }
    const payload = parseJson(sourcePayloadText.trim());
    if (!payload) {
      setError("Paste a valid JSON payload before running.");
      setRunning(false);
      return;
    }
    const trimmedMapping = mappingText.trim();
    const parsedMapping = trimmedMapping ? parseMappingSpec(trimmedMapping) : null;
    if (trimmedMapping && !parsedMapping) {
      setError("Mapping rules must be valid JSON.");
      setRunning(false);
      return;
    }
    try {
      const response = await ingestSchema(schemaId, {
        name: `Run ${name || "mapping"}`,
        sourceType: inputConfig.sourceType,
        data: payload,
        mapping: parsedMapping ?? undefined,
      });
      setRunJobId(response.job.id);
      setStatusMessage("Job created. Processing in background.");
      loadJobs();
    } catch {
      setError("Unable to run mapping.");
    } finally {
      setRunning(false);
    }
  };

  if (loading) {
    return (
      <AppShell>
        <div className="rounded-2xl bg-white p-6 text-sm text-slate-500 shadow-sm">
          Loading mapping...
        </div>
      </AppShell>
    );
  }

  if (!schema) {
    return (
      <AppShell>
        <div className="rounded-2xl bg-white p-6 text-sm text-slate-500 shadow-sm">
          {error ?? "Mapping not found."}
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
              Mapping
            </p>
            <h1 className="text-3xl font-semibold text-slate-900">
              {schema.name}
            </h1>
            <p className="mt-2 text-sm text-slate-600">
              Version {schema.version} • Updated {schema.updatedAt}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={handleRun}
              disabled={running}
              className="inline-flex items-center justify-center rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
            >
              {running ? "Running..." : "Run now"}
            </button>
            <Link
              href="/app/schemas"
              className="text-sm font-semibold text-slate-600 hover:text-slate-900"
            >
              Back to mappings
            </Link>
          </div>
        </div>

        <section className="grid gap-6 rounded-2xl bg-white p-6 shadow-sm lg:grid-cols-[1.2fr_0.8fr]">
          <div className="space-y-4">
            <div>
            <h2 className="text-lg font-semibold text-slate-900">
                1. Configure input
            </h2>
              <p className="text-sm text-slate-500">
                Choose where data comes from and paste a sample payload.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              {[
                { id: "file", label: "File/JSON" },
                { id: "api", label: "API" },
                { id: "cloud", label: "Cloud" },
              ].map((option) => (
                <button
                  key={option.id}
                  type="button"
                  onClick={() =>
                    setInputConfig((prev) => ({
                      ...prev,
                      sourceType: option.id as TransformerInputConfig["sourceType"],
                    }))
                  }
                  className={`rounded-full border px-4 py-2 text-sm font-semibold transition ${
                    inputConfig.sourceType === option.id
                      ? "border-slate-900 bg-slate-900 text-white"
                      : "border-slate-200 bg-white text-slate-600 hover:border-slate-400"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <div className="grid gap-3 rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
              {inputConfig.sourceType === "file" && (
                <>
                  <p className="font-semibold text-slate-900">Upload JSON</p>
                  <label className="flex items-center justify-between rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-600">
                    <span>
                      {fileName ? `Selected: ${fileName}` : "Choose a JSON file"}
            </span>
                    <input
                      type="file"
                      accept=".json,application/json"
                      className="text-xs"
                      onChange={(event) => {
                        const file = event.target.files?.[0];
                        if (file) {
                          handleFile(file);
                        }
                      }}
                    />
                  </label>
                </>
              )}
              {inputConfig.sourceType === "api" && (
                <>
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="https://api.example.com/orders"
                    value={inputConfig.endpoint ?? ""}
                    onChange={(event) =>
                      setInputConfig((prev) => ({
                        ...prev,
                        endpoint: event.target.value,
                      }))
                    }
                  />
                  <input
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    placeholder="API key"
                    value={inputConfig.apiKey ?? ""}
                    onChange={(event) =>
                      setInputConfig((prev) => ({
                        ...prev,
                        apiKey: event.target.value,
                      }))
                    }
                  />
                </>
              )}
              {inputConfig.sourceType === "cloud" && (
                <input
                  className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                  placeholder="s3://bucket/path/"
                  value={inputConfig.cloudPath ?? ""}
                  onChange={(event) =>
                    setInputConfig((prev) => ({
                      ...prev,
                      cloudPath: event.target.value,
                    }))
                  }
                />
              )}
            </div>
            <textarea
              className="min-h-[160px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
              placeholder="Paste a sample JSON payload here."
              value={sourcePayloadText}
              onChange={(event) => setSourcePayloadText(event.target.value)}
            />
            <button
              type="button"
              onClick={handleAnalyze}
              className="inline-flex items-center justify-center rounded-full border border-slate-300 bg-white px-4 py-2 text-xs font-semibold text-slate-900 hover:bg-slate-50"
            >
              Analyze input
            </button>
          </div>

          <aside className="space-y-4">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              <p className="text-sm font-semibold text-slate-900">
                Input analysis
              </p>
              <div className="mt-3 grid gap-3">
                <div className="flex items-center justify-between rounded-lg bg-white px-3 py-2">
                  <span>Fields detected</span>
                  <span className="font-semibold text-slate-900">
                    {metrics.fields}
                  </span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-white px-3 py-2">
                  <span>Records scanned</span>
                  <span className="font-semibold text-slate-900">
                    {metrics.rows}
                  </span>
                </div>
                <div className="flex items-center justify-between rounded-lg bg-white px-3 py-2">
                  <span>Issues flagged</span>
                  <span className="font-semibold text-slate-900">
                    {metrics.issues}
                  </span>
                </div>
              </div>
            </div>
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
          </aside>
        </section>

        <section className="grid gap-6 rounded-2xl bg-white p-6 shadow-sm lg:grid-cols-2">
          <div className="space-y-4">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                2. Configure output
              </h2>
              <p className="text-sm text-slate-500">
                Decide where transformed data should be delivered.
              </p>
            </div>
            <select
              className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700"
              value={outputConfig.destinationType}
              onChange={(event) =>
                setOutputConfig((prev) => ({
                  ...prev,
                  destinationType:
                    event.target.value as TransformerOutputConfig["destinationType"],
                }))
              }
            >
              <option value="webhook">Webhook</option>
              <option value="s3">S3</option>
              <option value="db">Database</option>
            </select>
            {outputConfig.destinationType === "webhook" && (
              <input
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
                placeholder="https://hooks.example.com/ingest"
                value={outputConfig.webhookUrl ?? ""}
                onChange={(event) =>
                  setOutputConfig((prev) => ({
                    ...prev,
                    webhookUrl: event.target.value,
                  }))
                }
              />
            )}
            {outputConfig.destinationType === "s3" && (
              <input
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
                placeholder="s3://bucket/path/"
                value={outputConfig.s3Path ?? ""}
                onChange={(event) =>
                  setOutputConfig((prev) => ({
                    ...prev,
                    s3Path: event.target.value,
                  }))
                }
              />
            )}
            {outputConfig.destinationType === "db" && (
              <input
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
                placeholder="postgres://user:pass@host:5432/db"
                value={outputConfig.connectionString ?? ""}
                onChange={(event) =>
                  setOutputConfig((prev) => ({
                    ...prev,
                    connectionString: event.target.value,
                  }))
                }
              />
            )}
          </div>

          <div className="space-y-4">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                4. Settings
              </h2>
              <p className="text-sm text-slate-500">
                Optional controls you can enable later.
              </p>
            </div>
            <label className="flex items-center justify-between rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              <span>Duplicate deletion</span>
              <input
                type="checkbox"
                checked={settings.dedupeEnabled}
                onChange={(event) =>
                  setSettings({ dedupeEnabled: event.target.checked })
                }
              />
            </label>
            <button
              type="button"
              onClick={handleSaveConfig}
              disabled={savingConfig}
              className="rounded-full bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
            >
              {savingConfig ? "Saving..." : "Save configuration"}
            </button>
          </div>
        </section>

        <section className="grid gap-6 rounded-2xl bg-white p-6 shadow-sm lg:grid-cols-[1.4fr_0.6fr]">
          <div className="space-y-4">
                <div>
              <h2 className="text-lg font-semibold text-slate-900">
                3. Edit schema
              </h2>
              <p className="text-sm text-slate-500">
                Update the target schema and optional mapping rules.
              </p>
            </div>
            <input
              className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="Mapping name"
            />
            <textarea
              className="min-h-[200px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
              value={schemaText}
              onChange={(event) => setSchemaText(event.target.value)}
            />
            <textarea
              className="min-h-[160px] w-full rounded-xl border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800"
              placeholder="Optional mapping rules (JSON)"
              value={mappingText}
              onChange={(event) => setMappingText(event.target.value)}
            />
            <button
              type="button"
              onClick={handleSaveSchema}
              disabled={savingSchema}
              className="rounded-full bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
            >
              {savingSchema ? "Saving..." : "Save schema"}
            </button>
          </div>

          <aside className="space-y-4">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                Jobs snapshot
              </h2>
              <p className="text-sm text-slate-500">
                {runJobId
                  ? `Latest job ID: ${runJobId}`
                  : "Create a job to see recent activity."}
                  </p>
                </div>
            <div className="rounded-xl border border-slate-200 bg-white p-4 text-sm">
              <div className="flex items-center justify-between">
                <p className="text-sm font-semibold text-slate-900">Jobs</p>
                {jobsLoading && (
                  <span className="text-xs text-slate-500">Loading...</span>
                )}
              </div>
              {jobs.length === 0 ? (
                <p className="mt-3 text-xs text-slate-500">
                  No jobs for this mapping yet.
                </p>
              ) : (
                <ul className="mt-3 space-y-2 text-xs text-slate-600">
                  {jobs.map((job) => (
                    <li
                      key={job.id}
                      className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50 px-3 py-2"
                    >
                      <div>
                        <p className="font-semibold text-slate-900">{job.name}</p>
                        <p className="text-[11px] text-slate-500">
                          {job.status} • {job.createdAt}
                        </p>
                      </div>
                      <Link
                        href={`/app/jobs/${job.id}`}
                        className="text-xs font-semibold text-slate-600 hover:text-slate-900"
                      >
                        View
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
              <Link
                href="/app/jobs"
                className="mt-4 inline-flex text-xs font-semibold text-slate-600 hover:text-slate-900"
              >
                View all jobs →
              </Link>
            </div>
          </aside>
        </section>

        {(error || statusMessage) && (
          <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm">
            {error && <p className="text-rose-600">{error}</p>}
            {statusMessage && <p className="text-emerald-700">{statusMessage}</p>}
          </div>
        )}
      </div>
    </AppShell>
  );
}
