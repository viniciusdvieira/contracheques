import { useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import { Eye, EyeOff, LogIn, Building2 } from "lucide-react";

const LoginPage = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const { toast } = useToast();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim() || !password.trim()) {
      toast({ title: "Preencha todos os campos", variant: "destructive" });
      return;
    }
    setLoading(true);
    const { error } = await supabase.auth.signInWithPassword({ email: email.trim(), password });
    if (error) {
      toast({
        title: "Erro ao entrar",
        description: "E-mail ou senha inválidos. Verifique seus dados e tente novamente.",
        variant: "destructive",
      });
    }
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: "var(--gradient-hero)" }}>
      <div className="w-full max-w-md px-4">
        {/* Logo / Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl mb-4" style={{ background: "var(--gradient-gold)" }}>
            <Building2 className="w-8 h-8 text-gold-foreground" />
          </div>
          <h1 className="text-3xl font-bold text-primary-foreground">Portal RH</h1>
          <p className="text-primary-foreground/60 mt-1 text-sm">Sistema de Contracheques</p>
        </div>

        {/* Card */}
        <div className="login-card rounded-xl p-8 animate-fade-in">
          <div className="mb-6">
            <h2 className="text-xl font-semibold text-foreground">Acesso ao Sistema</h2>
            <p className="text-muted-foreground text-sm mt-1">
              Utilize as credenciais fornecidas pelo setor de Recursos Humanos para acessar seus contracheques.
            </p>
          </div>

          <form onSubmit={handleLogin} className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="email" className="text-sm font-medium">E-mail corporativo</Label>
              <Input
                id="email"
                type="email"
                placeholder="seunome@empresa.com.br"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="h-11"
                autoComplete="email"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="password" className="text-sm font-medium">Senha</Label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  placeholder="Digite sua senha"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="h-11 pr-10"
                  autoComplete="current-password"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                >
                  {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            <Button
              type="submit"
              className="w-full h-11 text-sm font-semibold"
              style={{ background: "var(--gradient-hero)" }}
              disabled={loading}
            >
              {loading ? (
                <span className="flex items-center gap-2">
                  <span className="w-4 h-4 border-2 border-primary-foreground/30 border-t-primary-foreground rounded-full animate-spin" />
                  Entrando...
                </span>
              ) : (
                <span className="flex items-center gap-2">
                  <LogIn className="w-4 h-4" />
                  Entrar
                </span>
              )}
            </Button>
          </form>

          <div className="mt-6 p-3 rounded-lg bg-muted/60 border border-border/50">
            <p className="text-xs text-muted-foreground text-center leading-relaxed">
              Problemas com acesso? Entre em contato com o setor de <strong>Recursos Humanos</strong>.
            </p>
          </div>
        </div>

        <p className="text-center text-primary-foreground/40 text-xs mt-6">
          © {new Date().getFullYear()} Portal RH — Uso interno e confidencial
        </p>
      </div>
    </div>
  );
};

export default LoginPage;
