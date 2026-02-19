import { ReactNode } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Building2, FileText, KeyRound, LogOut, ShieldCheck, Menu, X } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";

const AppNavbar = ({ children }: { children: ReactNode }) => {
  const { signOut, isAdmin, user } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);

  const navItems = [
    { label: "Contracheques", icon: FileText, path: "/dashboard" },
    ...(isAdmin ? [{ label: "Administração", icon: ShieldCheck, path: "/admin" }] : []),
  ];

  return (
    <div className="flex min-h-screen bg-background">
      {/* Sidebar desktop */}
      <aside className="hidden md:flex flex-col w-64 nav-sidebar text-sidebar-foreground">
        <div className="p-6 border-b border-sidebar-border">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0" style={{ background: "var(--gradient-gold)" }}>
              <Building2 className="w-5 h-5 text-gold-foreground" />
            </div>
            <div>
              <p className="font-bold text-sm text-sidebar-foreground">Portal RH</p>
              <p className="text-xs text-sidebar-foreground/50">Contracheques</p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-4 space-y-1">
          {navItems.map((item) => (
            <button
              key={item.path}
              onClick={() => navigate(item.path)}
              className={cn(
                "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all",
                location.pathname === item.path
                  ? "bg-sidebar-accent text-gold"
                  : "text-sidebar-foreground/70 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground"
              )}
            >
              <item.icon className="w-4 h-4 flex-shrink-0" />
              {item.label}
            </button>
          ))}
        </nav>

        <div className="p-4 border-t border-sidebar-border space-y-1">
          <button
            onClick={() => navigate("/change-password")}
            className={cn(
              "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all",
              location.pathname === "/change-password"
                ? "bg-sidebar-accent text-gold"
                : "text-sidebar-foreground/70 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground"
            )}
          >
            <KeyRound className="w-4 h-4 flex-shrink-0" />
            Alterar Senha
          </button>

          <div className="px-3 py-2 rounded-lg bg-sidebar-accent/40">
            <p className="text-xs text-sidebar-foreground/50 truncate">{user?.email}</p>
          </div>

          <Button
            variant="ghost"
            onClick={signOut}
            className="w-full justify-start gap-3 text-sidebar-foreground/70 hover:text-destructive hover:bg-destructive/10 px-3"
          >
            <LogOut className="w-4 h-4" />
            Sair
          </Button>
        </div>
      </aside>

      {/* Mobile header */}
      <div className="md:hidden fixed top-0 left-0 right-0 z-50 nav-sidebar">
        <div className="flex items-center justify-between px-4 h-14">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: "var(--gradient-gold)" }}>
              <Building2 className="w-4 h-4 text-gold-foreground" />
            </div>
            <span className="font-bold text-sm text-sidebar-foreground">Portal RH</span>
          </div>
          <button onClick={() => setMobileOpen(!mobileOpen)} className="text-sidebar-foreground p-1">
            {mobileOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </div>
        {mobileOpen && (
          <div className="border-t border-sidebar-border px-4 py-3 space-y-1">
            {navItems.map((item) => (
              <button
                key={item.path}
                onClick={() => { navigate(item.path); setMobileOpen(false); }}
                className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-sidebar-foreground/70 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground transition-all"
              >
                <item.icon className="w-4 h-4" />
                {item.label}
              </button>
            ))}
            <button
              onClick={() => { navigate("/change-password"); setMobileOpen(false); }}
              className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-sidebar-foreground/70 hover:bg-sidebar-accent/60 hover:text-sidebar-foreground transition-all"
            >
              <KeyRound className="w-4 h-4" />
              Alterar Senha
            </button>
            <button
              onClick={signOut}
              className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-sidebar-foreground/70 hover:text-destructive hover:bg-destructive/10 transition-all"
            >
              <LogOut className="w-4 h-4" />
              Sair
            </button>
          </div>
        )}
      </div>

      {/* Main content */}
      <main className="flex-1 md:overflow-auto">
        <div className="md:hidden h-14" />
        {children}
      </main>
    </div>
  );
};

export default AppNavbar;
