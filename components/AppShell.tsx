import Link from "next/link";
import AuthGate from "@/components/AuthGate";
import AuthStatus from "@/components/AuthStatus";

const navItems = [
  { label: "Mappings", href: "/app" },
  { label: "Create mapping", href: "/app/schemas/new" },
  { label: "Billing", href: "/app/billing" },
];

type AppShellProps = {
  children: React.ReactNode;
};

export default function AppShell({ children }: AppShellProps) {
  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-4">
            <img
              src="/logo_cut.png"
              alt="AnyApi"
              className="h-16 w-auto"
            />
            <p className="text-xs text-slate-500">
              Simple mappings for any input
            </p>
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
