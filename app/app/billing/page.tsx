"use client";

import { useEffect, useMemo, useState } from "react";
import AppShell from "@/components/AppShell";
import {
  BillingStatus,
  createBillingPortalSession,
  createCheckoutSession,
  fetchBillingStatus,
} from "@/lib/billing";

const statusLabels: Record<string, string> = {
  active: "Active",
  trialing: "Trialing",
  past_due: "Past due",
  unpaid: "Unpaid",
  canceled: "Canceled",
};

export default function BillingPage() {
  const [status, setStatus] = useState<BillingStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const data = await fetchBillingStatus();
        setStatus(data);
        setError(null);
      } catch {
        setError("Unable to load billing status.");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const displayStatus = useMemo(() => {
    const raw = status?.stripeSubscriptionStatus;
    if (!raw) {
      return "No subscription";
    }
    return statusLabels[raw] ?? raw;
  }, [status]);

  const handleCheckout = async () => {
    setActionLoading(true);
    try {
      const { url } = await createCheckoutSession();
      window.location.href = url;
    } catch {
      setError("Unable to start checkout.");
      setActionLoading(false);
    }
  };

  const handlePortal = async () => {
    setActionLoading(true);
    try {
      const { url } = await createBillingPortalSession();
      window.location.href = url;
    } catch {
      setError("Unable to open billing portal.");
      setActionLoading(false);
    }
  };

  return (
    <AppShell>
      <section className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-semibold text-slate-900">Billing</h1>
          <p className="text-sm text-slate-600">
            Manage your 14-day free trial and $20/month unlimited plan.
          </p>
        </div>

        <div className="rounded-2xl bg-white p-8 shadow-sm">
          {loading ? (
            <p className="text-sm text-slate-500">Loading billing status...</p>
          ) : (
            <div className="flex flex-col gap-4">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                  Current status
                </p>
                <p className="text-lg font-semibold text-slate-900">
                  {displayStatus}
                </p>
                {status?.stripeTrialEndsAt && (
                  <p className="text-xs text-slate-500">
                    Trial ends on{" "}
                    {new Date(status.stripeTrialEndsAt).toLocaleDateString()}
                  </p>
                )}
                {status?.stripeCurrentPeriodEnd && (
                  <p className="text-xs text-slate-500">
                    Next renewal{" "}
                    {new Date(status.stripeCurrentPeriodEnd).toLocaleDateString()}
                  </p>
                )}
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  onClick={handleCheckout}
                  disabled={actionLoading}
                  className="rounded-full bg-slate-900 px-5 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-500"
                >
                  Start 14-day free trial
                </button>
                <button
                  onClick={handlePortal}
                  disabled={actionLoading}
                  className="rounded-full border border-slate-200 px-5 py-2 text-sm font-semibold text-slate-700 hover:border-slate-400 disabled:cursor-not-allowed disabled:text-slate-400"
                >
                  Manage subscription
                </button>
              </div>
              {error && (
                <p className="text-xs text-rose-600">
                  {error}
                </p>
              )}
            </div>
          )}
        </div>
      </section>
    </AppShell>
  );
}
