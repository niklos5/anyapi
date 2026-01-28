"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { getAccessToken, refresh } from "@/lib/auth";

type AuthGateProps = {
  children: React.ReactNode;
};

export default function AuthGate({ children }: AuthGateProps) {
  const router = useRouter();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const ensureAuth = async () => {
      const token = getAccessToken();
      if (token) {
        setReady(true);
        return;
      }
      try {
        await refresh();
        setReady(true);
      } catch {
        router.replace("/login");
      }
    };
    ensureAuth();
  }, [router]);

  if (!ready) {
    return (
      <div className="rounded-2xl bg-white p-6 shadow-sm">
        <p className="text-sm text-slate-500">Checking session...</p>
      </div>
    );
  }

  return <>{children}</>;
}
