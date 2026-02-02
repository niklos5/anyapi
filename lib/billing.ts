import { getAccessToken } from "@/lib/auth";

const AUTH_URL =
  process.env.NEXT_PUBLIC_AUTH_URL ?? "http://localhost:9000";

type RequestOptions = {
  method?: string;
  body?: string;
};

const doAuthRequest = async <T>(
  path: string,
  options: RequestOptions = {}
): Promise<T> => {
  const token = getAccessToken();
  if (!token) {
    throw new Error("Missing access token");
  }
  const response = await fetch(`${AUTH_URL}${path}`, {
    method: options.method ?? "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: options.body,
  });
  if (!response.ok) {
    throw new Error("Request failed");
  }
  return response.json() as Promise<T>;
};

export type BillingStatus = {
  stripeCustomerId?: string | null;
  stripeSubscriptionId?: string | null;
  stripeSubscriptionStatus?: string | null;
  stripeTrialEndsAt?: string | null;
  stripeCurrentPeriodEnd?: string | null;
  stripePriceId?: string | null;
};

export const createCheckoutSession = async (): Promise<{ url: string }> =>
  doAuthRequest("/billing/checkout");

export const createBillingPortalSession = async (): Promise<{ url: string }> =>
  doAuthRequest("/billing/portal");

export const fetchBillingStatus = async (): Promise<BillingStatus> =>
  doAuthRequest("/billing/status", { method: "GET" });
