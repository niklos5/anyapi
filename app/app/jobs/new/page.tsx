import { Suspense } from "react";
import JobNewClient from "./job-client";

export default function NewIngestionPage() {
  return (
    <Suspense
      fallback={
        <div className="rounded-2xl bg-white p-6 shadow-sm">
          <p className="text-sm text-slate-500">Loading ingestion form...</p>
        </div>
      }
    >
      <JobNewClient />
    </Suspense>
  );
}
