import Link from "next/link";
import AuthGate from "@/components/AuthGate";
import AuthStatus from "@/components/AuthStatus";

const navItems = [
  { label: "Dashboard", href: "/" },
  { label: "New Ingestion", href: "/jobs/new" },
];

type AppShellProps = {
  children: React.ReactNode;
};

export default function AppShell({ children }: AppShellProps) {
  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-slate-900 text-sm font-semibold text-white">
              AA
            </div>
            <div>
              <p className="text-sm font-semibold text-slate-900">AnyApi</p>
              <p className="text-xs text-slate-500">
                Ingestion and schema mapping
              </p>
            </div>
          </div>
          <nav className="flex items-center gap-4 text-sm font-medium text-slate-600">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="rounded-full px-3 py-2 transition hover:bg-slate-100 hover:text-slate-900"
              >
                {item.label}
              </Link>
            ))}
          </nav>
          <AuthStatus />
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-6 py-10">
        <AuthGate>{children}</AuthGate>
      </main>
    </div>
  );
}
