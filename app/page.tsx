"use client";

import Link from "next/link";

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-slate-950 text-white">
      <header className="border-b border-white/10">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-5">
          <div className="flex items-center gap-4">
            <img
              src="/logo-with-txt.png"
              alt="AnyApi"
              className="h-8 w-auto"
            />
            <p className="text-xs text-white/70">
              Agentic ingestion for any schema
            </p>
          </div>
          <div className="flex items-center gap-3 text-sm">
            <Link
              href="/login"
              className="rounded-full border border-white/20 px-4 py-2 font-semibold text-white/80 hover:border-white/50 hover:text-white"
            >
              Log in
            </Link>
            <Link
              href="/signup"
              className="rounded-full bg-white px-4 py-2 font-semibold text-slate-900 hover:bg-slate-100"
            >
              Sign up
            </Link>
          </div>
        </div>
      </header>

      <main className="mx-auto flex max-w-6xl flex-col gap-16 px-6 py-16">
        <section className="grid gap-10 lg:grid-cols-[1.1fr_0.9fr]">
          <div className="flex flex-col gap-6">
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-white/60">
              Any-to-any data mapping
            </p>
            <h1 className="text-4xl font-semibold leading-tight md:text-5xl">
              Turn messy data into usable schemas in hours, not weeks.
            </h1>
            <p className="text-lg text-white/70">
              AnyApi is an agentic ingestion engine that learns how to map
              partner data once and reuses it forever. Upload a schema, connect
              sources, and get clean data for products, analytics, and AI.
            </p>
            <div className="flex flex-wrap items-center gap-4">
              <Link
                href="/signup"
                className="rounded-full bg-white px-6 py-3 text-sm font-semibold text-slate-900 hover:bg-slate-100"
              >
                Start 14-day free trial
              </Link>
              <Link
                href="#how"
                className="text-sm font-semibold text-white/80 hover:text-white"
              >
                See how it works →
              </Link>
            </div>
            <div className="flex flex-wrap gap-4 text-xs text-white/60">
              <span>Schema-agnostic</span>
              <span>Reusable mappings</span>
              <span>API-first delivery</span>
              <span>14-day free trial</span>
            </div>
          </div>

          <div className="rounded-3xl border border-white/10 bg-white/5 p-6">
            <h2 className="text-sm font-semibold uppercase text-white/70">
              What teams use it for
            </h2>
            <div className="mt-6 space-y-4 text-sm text-white/75">
              <div>
                <p className="font-semibold text-white">Marketplaces</p>
                <p>Normalize supplier feeds from CSV, API, or PDFs.</p>
              </div>
              <div>
                <p className="font-semibold text-white">Data/Analytics</p>
                <p>Unify partner exports into a single warehouse schema.</p>
              </div>
              <div>
                <p className="font-semibold text-white">AI/ML</p>
                <p>Prepare heterogeneous training data fast.</p>
              </div>
              <div>
                <p className="font-semibold text-white">Integrations</p>
                <p>Ship customer onboarding without building custom ETL.</p>
              </div>
            </div>
          </div>
        </section>

        <section
          id="how"
          className="grid gap-8 rounded-3xl border border-white/10 bg-white/5 p-10 md:grid-cols-3"
        >
          {[
            {
              title: "Deploy schema",
              body: "Paste your target schema or sample data. AnyApi infers the canonical shape.",
            },
            {
              title: "Connect data",
              body: "Send any payload to the generated endpoint with your API key.",
            },
            {
              title: "Get clean output",
              body: "Mapped results are validated, versioned, and ready for reuse.",
            },
          ].map((item) => (
            <div key={item.title} className="space-y-3">
              <p className="text-sm font-semibold uppercase text-white/60">
                {item.title}
              </p>
              <p className="text-lg font-semibold text-white">{item.body}</p>
            </div>
          ))}
        </section>

        <section className="grid gap-10 md:grid-cols-[1.1fr_0.9fr]">
          <div className="space-y-4">
            <h2 className="text-3xl font-semibold">
              Stop rebuilding pipelines for every partner.
            </h2>
            <p className="text-white/70">
              AnyApi removes the manual mapping loop. Teams define mappings once
              and reuse them across ingestion, analytics, and AI workflows.
            </p>
          </div>
          <div className="rounded-3xl border border-white/10 bg-white/5 p-6 text-sm text-white/70">
            <p className="font-semibold text-white">What you get</p>
            <ul className="mt-4 space-y-2">
              <li>Reusable schema functions</li>
              <li>Agentic mapping with validation</li>
              <li>API-first ingestion with keys</li>
              <li>Auditability and mapping reuse</li>
            </ul>
          </div>
        </section>

        <section
          id="pricing"
          className="grid gap-8 rounded-3xl border border-white/10 bg-white/5 p-10 md:grid-cols-[1.1fr_0.9fr]"
        >
          <div className="space-y-4">
            <h2 className="text-3xl font-semibold">Simple, unlimited pricing.</h2>
            <p className="text-white/70">
              Start with a 14-day free trial. After that, it is $20/month for
              unlimited use across all services.
            </p>
            <div className="flex flex-wrap items-center gap-4">
              <Link
                href="/signup"
                className="rounded-full bg-white px-6 py-3 text-sm font-semibold text-slate-900 hover:bg-slate-100"
              >
                Start 14-day free trial
              </Link>
              <Link
                href="/signup"
                className="text-sm font-semibold text-white/80 hover:text-white"
              >
                View plan details →
              </Link>
            </div>
          </div>
          <div className="rounded-3xl border border-white/10 bg-slate-900/60 p-6 text-sm text-white/80">
            <p className="text-xs font-semibold uppercase text-white/60">
              Unlimited plan
            </p>
            <div className="mt-3 flex items-baseline gap-2">
              <span className="text-4xl font-semibold text-white">$20</span>
              <span className="text-white/60">/ month</span>
            </div>
            <p className="mt-3 text-white/70">14-day free trial included.</p>
            <ul className="mt-4 space-y-2">
              <li>Unlimited mappings and ingestion jobs</li>
              <li>All services included</li>
              <li>Unlimited usage volume</li>
              <li>Cancel anytime</li>
            </ul>
          </div>
        </section>

        <section className="rounded-3xl border border-white/10 bg-white/5 p-10 text-center">
          <h2 className="text-3xl font-semibold">
            Ready to turn messy data into clean APIs?
          </h2>
          <p className="mt-4 text-white/70">
            Start free, deploy your first schema in minutes, and share your API
            endpoint with partners.
          </p>
          <div className="mt-6 flex flex-wrap items-center justify-center gap-4">
            <Link
              href="/signup"
              className="rounded-full bg-white px-6 py-3 text-sm font-semibold text-slate-900 hover:bg-slate-100"
            >
              Create an account
            </Link>
            <Link
              href="/login"
              className="rounded-full border border-white/20 px-6 py-3 text-sm font-semibold text-white/80 hover:border-white/50 hover:text-white"
            >
              Log in
            </Link>
          </div>
        </section>
      </main>
    </div>
  );
}
