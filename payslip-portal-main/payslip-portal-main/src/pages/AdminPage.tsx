import { useEffect, useState, useRef } from "react";
import { supabase } from "@/integrations/supabase/client";
import AppNavbar from "@/components/AppNavbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
} from "@/components/ui/dialog";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import {
  Users, Search, Upload, CheckCircle2, Clock, Plus, Pencil, X, Eye,
} from "lucide-react";
import { format, parseISO } from "date-fns";
import { ptBR } from "date-fns/locale";

interface Employee {
  id: string;
  full_name: string;
  cpf: string | null;
  department: string | null;
  position: string | null;
  email?: string;
  payslip_count?: number;
  latest_payslip?: string | null;
}

interface Payslip {
  id: string;
  user_id: string;
  reference_month: string;
  file_url: string | null;
  file_name: string | null;
  issued_by_admin: boolean;
  viewed_at: string | null;
  downloaded_at: string | null;
}

const MONTHS = [
  { value: "01", label: "Janeiro" }, { value: "02", label: "Fevereiro" },
  { value: "03", label: "Março" }, { value: "04", label: "Abril" },
  { value: "05", label: "Maio" }, { value: "06", label: "Junho" },
  { value: "07", label: "Julho" }, { value: "08", label: "Agosto" },
  { value: "09", label: "Setembro" }, { value: "10", label: "Outubro" },
  { value: "11", label: "Novembro" }, { value: "12", label: "Dezembro" },
];

const currentYear = new Date().getFullYear();
const YEARS = Array.from({ length: 5 }, (_, i) => (currentYear - i).toString());

const AdminPage = () => {
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [payslips, setPayslips] = useState<Payslip[]>([]);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  // User dialog
  const [userDialog, setUserDialog] = useState(false);
  const [editingEmployee, setEditingEmployee] = useState<Employee | null>(null);
  const [userForm, setUserForm] = useState({ full_name: "", cpf: "", department: "", position: "", email: "", password: "" });
  const [userLoading, setUserLoading] = useState(false);

  // Payslip upload dialog
  const [uploadDialog, setUploadDialog] = useState(false);
  const [uploadEmployee, setUploadEmployee] = useState<Employee | null>(null);
  const [uploadMonth, setUploadMonth] = useState(MONTHS[(new Date().getMonth())].value);
  const [uploadYear, setUploadYear] = useState(currentYear.toString());
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadLoading, setUploadLoading] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  // Employee payslips view dialog
  const [viewDialog, setViewDialog] = useState(false);
  const [viewEmployee, setViewEmployee] = useState<Employee | null>(null);
  const [viewPayslips, setViewPayslips] = useState<Payslip[]>([]);

  const MONTH_NAMES: Record<string, string> = Object.fromEntries(MONTHS.map(m => [m.value, m.label]));

  const fetchData = async () => {
    setLoading(true);
    // Fetch all profiles + payslips
    const [profilesRes, payslipsRes] = await Promise.all([
      supabase.from("profiles").select("*").order("full_name"),
      supabase.from("payslips").select("*").order("reference_month", { ascending: false }),
    ]);

    if (profilesRes.data) {
      const emps: Employee[] = profilesRes.data.map(p => ({
        ...p,
        payslip_count: payslipsRes.data?.filter(ps => ps.user_id === p.id).length || 0,
        latest_payslip: payslipsRes.data?.find(ps => ps.user_id === p.id)?.reference_month || null,
      }));
      setEmployees(emps);
    }
    if (payslipsRes.data) setPayslips(payslipsRes.data);
    setLoading(false);
  };

  useEffect(() => { fetchData(); }, []);

  const filtered = employees.filter(e =>
    e.full_name.toLowerCase().includes(search.toLowerCase()) ||
    (e.cpf || "").includes(search) ||
    (e.department || "").toLowerCase().includes(search.toLowerCase())
  );

  // Create / Edit user
  const openCreateUser = () => {
    setEditingEmployee(null);
    setUserForm({ full_name: "", cpf: "", department: "", position: "", email: "", password: "" });
    setUserDialog(true);
  };

  const openEditUser = (emp: Employee) => {
    setEditingEmployee(emp);
    setUserForm({ full_name: emp.full_name, cpf: emp.cpf || "", department: emp.department || "", position: emp.position || "", email: emp.email || "", password: "" });
    setUserDialog(true);
  };

  const handleSaveUser = async () => {
    if (!userForm.full_name.trim()) {
      toast({ title: "Nome obrigatório", variant: "destructive" }); return;
    }
    setUserLoading(true);

    if (editingEmployee) {
      // Update profile only
      const { error } = await supabase.from("profiles").update({
        full_name: userForm.full_name.trim(),
        cpf: userForm.cpf || null,
        department: userForm.department || null,
        position: userForm.position || null,
      }).eq("id", editingEmployee.id);

      if (error) {
        toast({ title: "Erro ao atualizar", description: error.message, variant: "destructive" });
      } else {
        toast({ title: "Funcionário atualizado com sucesso!" });
        setUserDialog(false);
        fetchData();
      }
    } else {
      // Create new user via edge function
      if (!userForm.email.trim() || !userForm.password) {
        toast({ title: "E-mail e senha obrigatórios para novo usuário", variant: "destructive" });
        setUserLoading(false); return;
      }

      // Use admin createUser approach via supabase admin
      const { data: newUser, error: createError } = await supabase.functions.invoke("create-employee", {
        body: {
          email: userForm.email.trim(),
          password: userForm.password,
          full_name: userForm.full_name.trim(),
          cpf: userForm.cpf || null,
          department: userForm.department || null,
          position: userForm.position || null,
        },
      });

      if (createError || newUser?.error) {
        toast({ title: "Erro ao criar funcionário", description: createError?.message || newUser?.error, variant: "destructive" });
      } else {
        toast({ title: "Funcionário criado com sucesso!" });
        setUserDialog(false);
        fetchData();
      }
    }
    setUserLoading(false);
  };

  // Upload payslip
  const openUpload = (emp: Employee) => {
    setUploadEmployee(emp);
    setUploadFile(null);
    setUploadDialog(true);
  };

  const handleUpload = async () => {
    if (!uploadFile || !uploadEmployee) {
      toast({ title: "Selecione um arquivo PDF", variant: "destructive" }); return;
    }
    setUploadLoading(true);
    const refMonth = `${uploadYear}-${uploadMonth}-01`;
    const filePath = `${uploadEmployee.id}/${uploadYear}-${uploadMonth}.pdf`;

    const { error: storageError } = await supabase.storage.from("payslips").upload(filePath, uploadFile, { upsert: true });
    if (storageError) {
      toast({ title: "Erro ao enviar arquivo", description: storageError.message, variant: "destructive" });
      setUploadLoading(false); return;
    }

    // Check if payslip for this month already exists
    const existing = payslips.find(p => p.user_id === uploadEmployee.id && p.reference_month === refMonth);
    if (existing) {
      await supabase.from("payslips").update({ file_url: filePath, file_name: uploadFile.name, issued_by_admin: true }).eq("id", existing.id);
    } else {
      await supabase.from("payslips").insert({
        user_id: uploadEmployee.id,
        reference_month: refMonth,
        file_url: filePath,
        file_name: uploadFile.name,
        issued_by_admin: true,
      });
    }

    toast({ title: "Contracheque enviado com sucesso!" });
    setUploadDialog(false);
    setUploadFile(null);
    fetchData();
    setUploadLoading(false);
  };

  // View employee payslips
  const openViewPayslips = (emp: Employee) => {
    setViewEmployee(emp);
    setViewPayslips(payslips.filter(p => p.user_id === emp.id));
    setViewDialog(true);
  };

  const getMonthLabel = (dateStr: string) => {
    const d = parseISO(dateStr);
    const m = String(d.getMonth() + 1).padStart(2, "0");
    return `${MONTH_NAMES[m]} / ${d.getFullYear()}`;
  };

  return (
    <AppNavbar>
      <div className="p-6 md:p-8 max-w-6xl animate-fade-in">
        {/* Header */}
        <div className="flex items-center justify-between mb-6 gap-4 flex-wrap">
          <div>
            <div className="flex items-center gap-3 mb-1">
              <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ background: "var(--gradient-gold)" }}>
                <Users className="w-5 h-5 text-gold-foreground" />
              </div>
              <h1 className="text-2xl font-bold text-foreground">Administração</h1>
            </div>
            <p className="text-muted-foreground text-sm">Gerencie funcionários e contracheques.</p>
          </div>
          <Button onClick={openCreateUser} className="gap-2 font-semibold" style={{ background: "var(--gradient-hero)" }}>
            <Plus className="w-4 h-4" /> Novo Funcionário
          </Button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-6">
          <div className="card-elevated rounded-xl border border-border p-4">
            <p className="text-xs text-muted-foreground mb-1 uppercase tracking-wider">Total de Funcionários</p>
            <p className="text-2xl font-bold text-foreground">{employees.length}</p>
          </div>
          <div className="card-elevated rounded-xl border border-border p-4">
            <p className="text-xs text-muted-foreground mb-1 uppercase tracking-wider">Com Contracheque</p>
            <p className="text-2xl font-bold text-success">{employees.filter(e => (e.payslip_count || 0) > 0).length}</p>
          </div>
          <div className="card-elevated rounded-xl border border-border p-4 col-span-2 md:col-span-1">
            <p className="text-xs text-muted-foreground mb-1 uppercase tracking-wider">Sem Contracheque</p>
            <p className="text-2xl font-bold" style={{ color: "hsl(38 80% 35%)" }}>{employees.filter(e => (e.payslip_count || 0) === 0).length}</p>
          </div>
        </div>

        {/* Search */}
        <div className="relative mb-5">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            placeholder="Pesquisar por nome, CPF ou setor..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 h-10"
          />
        </div>

        {/* Table */}
        {loading ? (
          <div className="space-y-3">
            {[1, 2, 3, 4].map(i => <div key={i} className="h-14 rounded-xl bg-muted/40 animate-pulse" />)}
          </div>
        ) : (
          <div className="card-elevated rounded-xl border border-border overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-muted/30">
                  <th className="text-left px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider">Funcionário</th>
                  <th className="text-left px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider hidden md:table-cell">Setor</th>
                  <th className="text-left px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider hidden sm:table-cell">Status</th>
                  <th className="text-right px-5 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider">Ações</th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="px-5 py-10 text-center text-muted-foreground text-sm">
                      Nenhum funcionário encontrado.
                    </td>
                  </tr>
                ) : filtered.map((emp, idx) => (
                  <tr key={emp.id} className={`payslip-row border-b border-border last:border-0 ${idx % 2 === 0 ? "" : "bg-muted/10"}`}>
                    <td className="px-5 py-3.5">
                      <div>
                        <p className="font-medium text-sm text-foreground">{emp.full_name}</p>
                        {emp.cpf && <p className="text-xs text-muted-foreground">CPF: {emp.cpf}</p>}
                        {emp.position && <p className="text-xs text-muted-foreground">{emp.position}</p>}
                      </div>
                    </td>
                    <td className="px-5 py-3.5 hidden md:table-cell">
                      <p className="text-sm text-foreground">{emp.department || "—"}</p>
                    </td>
                    <td className="px-5 py-3.5 hidden sm:table-cell">
                      {(emp.payslip_count || 0) > 0 ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium status-issued">
                          <CheckCircle2 className="w-3 h-3" /> {emp.payslip_count} emitido(s)
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium status-pending">
                          <Clock className="w-3 h-3" /> Nenhum
                        </span>
                      )}
                    </td>
                    <td className="px-5 py-3.5">
                      <div className="flex items-center justify-end gap-1.5">
                        <Button variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={() => openViewPayslips(emp)} title="Ver contracheques">
                          <Eye className="w-3.5 h-3.5" />
                        </Button>
                        <Button variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={() => openEditUser(emp)} title="Editar funcionário">
                          <Pencil className="w-3.5 h-3.5" />
                        </Button>
                        <Button size="sm" className="h-8 text-xs gap-1.5" style={{ background: "var(--gradient-hero)" }} onClick={() => openUpload(emp)}>
                          <Upload className="w-3.5 h-3.5" />
                          <span className="hidden sm:inline">Enviar</span>
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Create/Edit User Dialog */}
      <Dialog open={userDialog} onOpenChange={setUserDialog}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>{editingEmployee ? "Editar Funcionário" : "Novo Funcionário"}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label className="text-sm">Nome completo *</Label>
              <Input placeholder="Nome completo" value={userForm.full_name} onChange={e => setUserForm(f => ({ ...f, full_name: e.target.value }))} />
            </div>
            {!editingEmployee && (
              <>
                <div className="space-y-1.5">
                  <Label className="text-sm">E-mail *</Label>
                  <Input type="email" placeholder="email@empresa.com" value={userForm.email} onChange={e => setUserForm(f => ({ ...f, email: e.target.value }))} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-sm">Senha inicial *</Label>
                  <Input type="password" placeholder="Senha temporária" value={userForm.password} onChange={e => setUserForm(f => ({ ...f, password: e.target.value }))} />
                  <p className="text-xs text-muted-foreground">O funcionário deverá alterar no primeiro acesso.</p>
                </div>
              </>
            )}
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label className="text-sm">CPF</Label>
                <Input placeholder="000.000.000-00" value={userForm.cpf} onChange={e => setUserForm(f => ({ ...f, cpf: e.target.value }))} />
              </div>
              <div className="space-y-1.5">
                <Label className="text-sm">Cargo</Label>
                <Input placeholder="Cargo" value={userForm.position} onChange={e => setUserForm(f => ({ ...f, position: e.target.value }))} />
              </div>
            </div>
            <div className="space-y-1.5">
              <Label className="text-sm">Departamento / Setor</Label>
              <Input placeholder="Ex: Financeiro, TI, RH..." value={userForm.department} onChange={e => setUserForm(f => ({ ...f, department: e.target.value }))} />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setUserDialog(false)}>Cancelar</Button>
            <Button onClick={handleSaveUser} disabled={userLoading} style={{ background: "var(--gradient-hero)" }}>
              {userLoading ? "Salvando..." : editingEmployee ? "Salvar" : "Criar Funcionário"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Upload Payslip Dialog */}
      <Dialog open={uploadDialog} onOpenChange={setUploadDialog}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Enviar Contracheque</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <p className="text-sm text-muted-foreground">
              Funcionário: <strong className="text-foreground">{uploadEmployee?.full_name}</strong>
            </p>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label className="text-sm">Mês</Label>
                <Select value={uploadMonth} onValueChange={setUploadMonth}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {MONTHS.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1.5">
                <Label className="text-sm">Ano</Label>
                <Select value={uploadYear} onValueChange={setUploadYear}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {YEARS.map(y => <SelectItem key={y} value={y}>{y}</SelectItem>)}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="space-y-1.5">
              <Label className="text-sm">Arquivo PDF</Label>
              <div
                className="border-2 border-dashed border-border rounded-lg p-6 text-center cursor-pointer hover:border-primary/40 transition-colors"
                onClick={() => fileRef.current?.click()}
              >
                {uploadFile ? (
                  <div className="flex items-center justify-center gap-2">
                    <CheckCircle2 className="w-4 h-4 text-success" />
                    <span className="text-sm font-medium text-foreground truncate max-w-[200px]">{uploadFile.name}</span>
                    <button onClick={(e) => { e.stopPropagation(); setUploadFile(null); }} className="text-muted-foreground hover:text-destructive">
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                ) : (
                  <div>
                    <Upload className="w-6 h-6 text-muted-foreground mx-auto mb-2" />
                    <p className="text-sm text-muted-foreground">Clique para selecionar o arquivo PDF</p>
                  </div>
                )}
              </div>
              <input ref={fileRef} type="file" accept=".pdf,application/pdf" className="hidden" onChange={e => setUploadFile(e.target.files?.[0] || null)} />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setUploadDialog(false)}>Cancelar</Button>
            <Button onClick={handleUpload} disabled={uploadLoading || !uploadFile} style={{ background: "var(--gradient-hero)" }}>
              {uploadLoading ? "Enviando..." : "Enviar Contracheque"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* View Employee Payslips Dialog */}
      <Dialog open={viewDialog} onOpenChange={setViewDialog}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Contracheques — {viewEmployee?.full_name}</DialogTitle>
          </DialogHeader>
          <div className="max-h-80 overflow-y-auto">
            {viewPayslips.length === 0 ? (
              <p className="text-sm text-muted-foreground py-6 text-center">Nenhum contracheque emitido.</p>
            ) : (
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-2 text-xs font-semibold text-muted-foreground">Competência</th>
                    <th className="text-left py-2 text-xs font-semibold text-muted-foreground">Visualizado</th>
                    <th className="text-left py-2 text-xs font-semibold text-muted-foreground">Baixado</th>
                  </tr>
                </thead>
                <tbody>
                  {viewPayslips.map(p => (
                    <tr key={p.id} className="border-b border-border last:border-0">
                      <td className="py-2.5 text-sm font-medium">{getMonthLabel(p.reference_month)}</td>
                      <td className="py-2.5">
                        {p.viewed_at ? (
                          <span className="text-xs status-issued px-2 py-0.5 rounded-full">Sim</span>
                        ) : (
                          <span className="text-xs text-muted-foreground">Não</span>
                        )}
                      </td>
                      <td className="py-2.5">
                        {p.downloaded_at ? (
                          <span className="text-xs status-issued px-2 py-0.5 rounded-full">Sim</span>
                        ) : (
                          <span className="text-xs text-muted-foreground">Não</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setViewDialog(false)}>Fechar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </AppNavbar>
  );
};

export default AdminPage;
