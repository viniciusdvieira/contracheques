import { useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import { Eye, EyeOff, ShieldCheck, Building2 } from "lucide-react";

const FirstPasswordChangePage = () => {
  const { refreshProfile, signOut } = useAuth();
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showNew, setShowNew] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [loading, setLoading] = useState(false);
  const { toast } = useToast();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (newPassword.length < 8) {
      toast({ title: "Senha muito curta", description: "A senha deve ter no mínimo 8 caracteres.", variant: "destructive" });
      return;
    }
    if (newPassword !== confirmPassword) {
      toast({ title: "Senhas não coincidem", description: "A confirmação da senha não está correta.", variant: "destructive" });
      return;
    }
    setLoading(true);
    const { error: pwError } = await supabase.auth.updateUser({ password: newPassword });
    if (pwError) {
      toast({ title: "Erro ao atualizar senha", description: pwError.message, variant: "destructive" });
      setLoading(false);
      return;
    }
    const { data: { user } } = await supabase.auth.getUser();
    if (user) {
      await supabase.from("profiles").update({ must_change_password: false }).eq("id", user.id);
    }
    await refreshProfile();
    toast({ title: "Senha atualizada com sucesso!", description: "Bem-vindo ao Portal RH." });
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center" style={{ background: "var(--gradient-hero)" }}>
      <div className="w-full max-w-md px-4">
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl mb-4" style={{ background: "var(--gradient-gold)" }}>
            <Building2 className="w-8 h-8 text-gold-foreground" />
          </div>
          <h1 className="text-3xl font-bold text-primary-foreground">Portal RH</h1>
          <p className="text-primary-foreground/60 mt-1 text-sm">Sistema de Contracheques</p>
        </div>

        <div className="login-card rounded-xl p-8 animate-fade-in">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ background: "var(--gradient-gold)" }}>
              <ShieldCheck className="w-5 h-5 text-gold-foreground" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Redefinição de Senha</h2>
              <p className="text-muted-foreground text-xs">Necessária no primeiro acesso</p>
            </div>
          </div>

          <div className="mb-5 p-3 rounded-lg border border-warning/30 bg-warning/8">
            <p className="text-sm text-foreground leading-relaxed">
              Por segurança, você precisa criar uma senha pessoal antes de continuar. Sua senha deve ter no mínimo <strong>8 caracteres</strong>.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="newPassword" className="text-sm font-medium">Nova senha</Label>
              <div className="relative">
                <Input
                  id="newPassword"
                  type={showNew ? "text" : "password"}
                  placeholder="Mínimo 8 caracteres"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="h-11 pr-10"
                />
                <button type="button" onClick={() => setShowNew(!showNew)} className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors">
                  {showNew ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="confirmPassword" className="text-sm font-medium">Confirmar nova senha</Label>
              <div className="relative">
                <Input
                  id="confirmPassword"
                  type={showConfirm ? "text" : "password"}
                  placeholder="Repita a nova senha"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="h-11 pr-10"
                />
                <button type="button" onClick={() => setShowConfirm(!showConfirm)} className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors">
                  {showConfirm ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                </button>
              </div>
            </div>

            <Button type="submit" className="w-full h-11 text-sm font-semibold" style={{ background: "var(--gradient-hero)" }} disabled={loading}>
              {loading ? (
                <span className="flex items-center gap-2">
                  <span className="w-4 h-4 border-2 border-primary-foreground/30 border-t-primary-foreground rounded-full animate-spin" />
                  Salvando...
                </span>
              ) : "Definir Senha e Continuar"}
            </Button>
          </form>

          <button onClick={signOut} className="w-full mt-4 text-xs text-muted-foreground hover:text-foreground transition-colors text-center">
            Sair e voltar ao login
          </button>
        </div>
      </div>
    </div>
  );
};

export default FirstPasswordChangePage;
