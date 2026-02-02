"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { getAccessToken, logout } from "@/lib/auth";

export default function AuthStatus() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);

  useEffect(() => {
    setAuthenticated(Boolean(getAccessToken()));
  }, []);

  const handleLogout = async () => {
    setLoading(true);
    try {
      await logout();
    } finally {
      router.replace("/login");
    }
  };

  return (
    <div className="flex items-center gap-3 text-sm text-slate-500">
      {authenticated ? (
        <>
          <div className="flex items-center gap-2">
            <div className="h-2 w-2 rounded-full bg-emerald-500" />
            Signed in
          </div>
          <button
            onClick={handleLogout}
            className="rounded-full border border-slate-200 px-3 py-1 text-xs font-semibold text-slate-600 hover:border-slate-400"
            disabled={loading}
          >
            {loading ? "Signing out..." : "Sign out"}
          </button>
        </>
      ) : (
        <button
          onClick={() => router.replace("/login")}
          className="rounded-full border border-slate-200 px-3 py-1 text-xs font-semibold text-slate-600 hover:border-slate-400"
        >
          Sign in
        </button>
      )}
    </div>
  );
}
