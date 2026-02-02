"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { bootstrapSession, getAccessToken } from "@/lib/auth";

type AuthGateProps = {
  children: React.ReactNode;
};

export default function AuthGate({ children }: AuthGateProps) {
  const router = useRouter();
  const [checking, setChecking] = useState(true);
  const [authorized, setAuthorized] = useState(false);

  useEffect(() => {
    const verify = async () => {
      await bootstrapSession();
      const token = getAccessToken();
      if (!token) {
        router.replace("/login");
        setAuthorized(false);
      } else {
        setAuthorized(true);
      }
      setChecking(false);
    };
    verify();
  }, [router]);

  if (checking) {
    return (
      <div className="rounded-2xl bg-white p-6 text-sm text-slate-500 shadow-sm">
        Verifying session...
      </div>
    );
  }

  if (!authorized) {
    return null;
  }

  return <>{children}</>;
}
