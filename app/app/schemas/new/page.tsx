import { Suspense } from "react";
import NewSchemaClient from "./schema-client";

export default function NewSchemaPage() {
  return (
    <Suspense fallback={<div className="p-6 text-sm text-slate-500">Loading…</div>}>
      <NewSchemaClient />
    </Suspense>
  );
}
