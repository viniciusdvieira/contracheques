import { useEffect, useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/contexts/AuthContext";
import AppNavbar from "@/components/AppNavbar";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { FileText, Eye, Download, Calendar, CheckCircle2, Clock } from "lucide-react";
import { format, parseISO } from "date-fns";
import { ptBR } from "date-fns/locale";

interface Payslip {
  id: string;
  reference_month: string;
  file_url: string | null;
  file_name: string | null;
  issued_by_admin: boolean;
  viewed_at: string | null;
  downloaded_at: string | null;
}

const MONTH_NAMES = [
  "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
  "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
];

const DashboardPage = () => {
  const { user } = useAuth();
  const [payslips, setPayslips] = useState<Payslip[]>([]);
  const [profile, setProfile] = useState<{ full_name: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  useEffect(() => {
    if (!user) return;
    const fetchData = async () => {
      const [payslipsRes, profileRes] = await Promise.all([
        supabase.from("payslips").select("*").eq("user_id", user.id).order("reference_month", { ascending: false }),
        supabase.from("profiles").select("full_name").eq("id", user.id).single(),
      ]);
      if (payslipsRes.data) setPayslips(payslipsRes.data);
      if (profileRes.data) setProfile(profileRes.data);
      setLoading(false);
    };
    fetchData();
  }, [user]);

  const getMonthLabel = (dateStr: string) => {
    const d = parseISO(dateStr);
    return `${MONTH_NAMES[d.getMonth()]} / ${d.getFullYear()}`;
  };

  const handleView = async (payslip: Payslip) => {
    if (!payslip.file_url) {
      toast({ title: "Arquivo não disponível", description: "Este contracheque ainda não possui arquivo anexado.", variant: "destructive" });
      return;
    }
    // Mark as viewed
    if (!payslip.viewed_at) {
      await supabase.from("payslips").update({ viewed_at: new Date().toISOString() }).eq("id", payslip.id);
      setPayslips(prev => prev.map(p => p.id === payslip.id ? { ...p, viewed_at: new Date().toISOString() } : p));
    }
    // Get signed URL
    const { data } = await supabase.storage.from("payslips").createSignedUrl(payslip.file_url, 3600);
    if (data?.signedUrl) window.open(data.signedUrl, "_blank");
    else toast({ title: "Erro ao abrir arquivo", variant: "destructive" });
  };

  const handleDownload = async (payslip: Payslip) => {
    if (!payslip.file_url) {
      toast({ title: "Arquivo não disponível", variant: "destructive" });
      return;
    }
    if (!payslip.downloaded_at) {
      await supabase.from("payslips").update({ downloaded_at: new Date().toISOString() }).eq("id", payslip.id);
      setPayslips(prev => prev.map(p => p.id === payslip.id ? { ...p, downloaded_at: new Date().toISOString() } : p));
    }
    const { data } = await supabase.storage.from("payslips").createSignedUrl(payslip.file_url, 3600);
    if (data?.signedUrl) {
      const a = document.createElement("a");
      a.href = data.signedUrl;
      a.download = payslip.file_name || `contracheque-${payslip.reference_month}.pdf`;
      a.click();
    } else toast({ title: "Erro ao baixar arquivo", variant: "destructive" });
  };

  // Group by year
  const groupedByYear = payslips.reduce((acc, p) => {
    const year = parseISO(p.reference_month).getFullYear().toString();
    if (!acc[year]) acc[year] = [];
    acc[year].push(p);
    return acc;
  }, {} as Record<string, Payslip[]>);

  const years = Object.keys(groupedByYear).sort((a, b) => Number(b) - Number(a));

  return (
    <AppNavbar>
      <div className="p-6 md:p-8 max-w-4xl animate-fade-in">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-foreground">
            Olá, {profile?.full_name?.split(" ")[0] || "Funcionário"} 👋
          </h1>
          <p className="text-muted-foreground text-sm mt-1">Aqui estão seus contracheques disponíveis.</p>
        </div>

        {loading ? (
          <div className="space-y-3">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-16 rounded-xl bg-muted/50 animate-pulse" />
            ))}
          </div>
        ) : payslips.length === 0 ? (
          <div className="card-elevated rounded-xl p-12 border border-border text-center">
            <FileText className="w-12 h-12 text-muted-foreground/40 mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-foreground mb-2">Nenhum contracheque disponível</h3>
            <p className="text-muted-foreground text-sm">Seus contracheques aparecerão aqui quando emitidos pelo RH.</p>
          </div>
        ) : (
          <div className="space-y-8">
            {years.map(year => (
              <div key={year}>
                <div className="flex items-center gap-3 mb-4">
                  <Calendar className="w-4 h-4 text-muted-foreground" />
                  <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">{year}</h2>
                  <div className="flex-1 border-t border-border" />
                </div>

                <div className="card-elevated rounded-xl border border-border overflow-hidden">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border bg-muted/30">
                        <th className="text-left px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider">Competência</th>
                        <th className="text-left px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider hidden sm:table-cell">Status</th>
                        <th className="text-right px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider">Ações</th>
                      </tr>
                    </thead>
                    <tbody>
                      {groupedByYear[year].map((payslip, idx) => (
                        <tr key={payslip.id} className={`payslip-row border-b border-border last:border-0 ${idx % 2 === 0 ? "" : "bg-muted/10"}`}>
                          <td className="px-5 py-4">
                            <div className="flex items-center gap-3">
                              <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0" style={{ background: "var(--gradient-hero)" }}>
                                <FileText className="w-4 h-4 text-primary-foreground" />
                              </div>
                              <div>
                                <p className="font-medium text-sm text-foreground">{getMonthLabel(payslip.reference_month)}</p>
                                {payslip.issued_by_admin && (
                                  <p className="text-xs text-muted-foreground">Emitido pelo RH</p>
                                )}
                              </div>
                            </div>
                          </td>
                          <td className="px-5 py-4 hidden sm:table-cell">
                            {payslip.downloaded_at ? (
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium status-issued">
                                <CheckCircle2 className="w-3 h-3" /> Baixado
                              </span>
                            ) : payslip.viewed_at ? (
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium status-pending">
                                <Eye className="w-3 h-3" /> Visualizado
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-muted text-muted-foreground border border-border">
                                <Clock className="w-3 h-3" /> Pendente
                              </span>
                            )}
                          </td>
                          <td className="px-5 py-4">
                            <div className="flex items-center justify-end gap-2">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleView(payslip)}
                                className="h-8 text-xs gap-1.5"
                                disabled={!payslip.file_url}
                              >
                                <Eye className="w-3.5 h-3.5" />
                                <span className="hidden sm:inline">Visualizar</span>
                              </Button>
                              <Button
                                size="sm"
                                onClick={() => handleDownload(payslip)}
                                className="h-8 text-xs gap-1.5"
                                style={{ background: "var(--gradient-hero)" }}
                                disabled={!payslip.file_url}
                              >
                                <Download className="w-3.5 h-3.5" />
                                <span className="hidden sm:inline">Baixar</span>
                              </Button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </AppNavbar>
  );
};

export default DashboardPage;
