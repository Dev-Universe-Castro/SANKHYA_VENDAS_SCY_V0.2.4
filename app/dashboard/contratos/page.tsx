"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"
import { toast } from "sonner"
import { Plus, Edit, Trash2, Eye, EyeOff } from "lucide-react"
import { Switch } from "@/components/ui/switch"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  SANKHYA_TOKEN?: string
  SANKHYA_APPKEY?: string
  SANKHYA_USERNAME?: string
  SANKHYA_PASSWORD?: string
  OAUTH_CLIENT_ID?: string
  OAUTH_CLIENT_SECRET?: string
  OAUTH_X_TOKEN?: string
  GEMINI_API_KEY: string
  AI_PROVEDOR?: string
  AI_MODELO?: string
  AI_CREDENTIAL?: string
  ATIVO: boolean
  IS_SANDBOX?: boolean
  LICENCAS: number
  AUTH_TYPE?: 'LEGACY' | 'OAUTH2'
}

interface FormData {
  ID_EMPRESA?: number
  EMPRESA: string
  CNPJ: string
  AUTH_TYPE?: 'LEGACY' | 'OAUTH2'
  SANKHYA_TOKEN?: string
  SANKHYA_APPKEY?: string
  SANKHYA_USERNAME?: string
  SANKHYA_PASSWORD?: string
  OAUTH_CLIENT_ID?: string
  OAUTH_CLIENT_SECRET?: string
  OAUTH_X_TOKEN?: string
  GEMINI_API_KEY: string
  AI_PROVEDOR?: string
  AI_MODELO?: string
  AI_CREDENTIAL?: string
  ATIVO: boolean
  IS_SANDBOX: boolean
  LICENCAS: number
}


export default function ContratosPage() {
  const router = useRouter()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [isEditing, setIsEditing] = useState(false)
  const [currentContrato, setCurrentContrato] = useState<Contrato | null>(null)
  const [showPasswords, setShowPasswords] = useState({
    token: false,
    appkey: false,
    password: false,
    gemini: false,
    clientId: false,
    clientSecret: false,
    xToken: false
  })
  const [editingId, setEditingId] = useState<number | null>(null);

  const initialFormData: FormData = {
    EMPRESA: '',
    CNPJ: '',
    AUTH_TYPE: 'LEGACY',
    SANKHYA_TOKEN: '',
    SANKHYA_APPKEY: '',
    SANKHYA_USERNAME: '',
    SANKHYA_PASSWORD: '',
    OAUTH_CLIENT_ID: '',
    OAUTH_CLIENT_SECRET: '',
    OAUTH_X_TOKEN: '',
    GEMINI_API_KEY: '',
    AI_PROVEDOR: 'Gemini',
    AI_MODELO: 'gemini-2.0-flash',
    AI_CREDENTIAL: '',
    ATIVO: true,
    IS_SANDBOX: true,
    LICENCAS: 0
  }

  const [formData, setFormData] = useState<FormData>(initialFormData)


  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== "Administrador") {
      router.push("/dashboard")
      return
    }
    carregarContratos()
  }, [router])

  const carregarContratos = async () => {
    try {
      const response = await fetch("/api/contratos")
      if (response.ok) {
        const data = await response.json()
        setContratos(data)
      }
    } catch (error) {
      console.error("Erro ao carregar contratos:", error)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!formData.EMPRESA || !formData.CNPJ) {
      toast.error("Preencha os campos obrigatórios")
      return
    }

    try {
      const url = isEditing ? "/api/contratos/atualizar" : "/api/contratos/criar"

      const payload = isEditing ? {
        id: formData.ID_EMPRESA,
        empresa: formData.EMPRESA,
        cnpj: formData.CNPJ,
        authType: formData.AUTH_TYPE,
        sankhyaToken: formData.SANKHYA_TOKEN,
        sankhyaAppkey: formData.SANKHYA_APPKEY,
        sankhyaUsername: formData.SANKHYA_USERNAME,
        sankhyaPassword: formData.SANKHYA_PASSWORD,
        oauthClientId: formData.OAUTH_CLIENT_ID,
        oauthClientSecret: formData.OAUTH_CLIENT_SECRET,
        oauthXToken: formData.OAUTH_X_TOKEN,
        geminiApiKey: formData.GEMINI_API_KEY,
        aiProvedor: formData.AI_PROVEDOR,
        aiModelo: formData.AI_MODELO,
        aiCredential: formData.AI_CREDENTIAL,
        ativo: formData.ATIVO,
        isSandbox: formData.IS_SANDBOX,
        licencas: formData.LICENCAS
      } : {
        empresa: formData.EMPRESA,
        cnpj: formData.CNPJ,
        authType: formData.AUTH_TYPE,
        sankhyaToken: formData.SANKHYA_TOKEN,
        sankhyaAppkey: formData.SANKHYA_APPKEY,
        sankhyaUsername: formData.SANKHYA_USERNAME,
        sankhyaPassword: formData.SANKHYA_PASSWORD,
        oauthClientId: formData.OAUTH_CLIENT_ID,
        oauthClientSecret: formData.OAUTH_CLIENT_SECRET,
        oauthXToken: formData.OAUTH_X_TOKEN,
        geminiApiKey: formData.GEMINI_API_KEY,
        aiProvedor: formData.AI_PROVEDOR,
        aiModelo: formData.AI_MODELO,
        aiCredential: formData.AI_CREDENTIAL,
        ativo: formData.ATIVO,
        isSandbox: formData.IS_SANDBOX,
        licencas: formData.LICENCAS
      }

      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      })

      if (response.ok) {
        toast.success(isEditing ? "Contrato atualizado com sucesso" : "Contrato criado com sucesso")
        setIsModalOpen(false)
        resetForm()
        await carregarContratos()
      } else {
        const error = await response.json()
        toast.error(error.error || "Erro ao salvar contrato")
      }
    } catch (error) {
      toast.error("Erro ao salvar contrato")
    }
  }

  const handleEdit = (contrato: Contrato) => {
    setCurrentContrato(contrato)
    setFormData({
      ID_EMPRESA: contrato.ID_EMPRESA,
      EMPRESA: contrato.EMPRESA,
      CNPJ: contrato.CNPJ,
      AUTH_TYPE: contrato.AUTH_TYPE || 'LEGACY',
      SANKHYA_TOKEN: contrato.SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: contrato.SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: contrato.SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: contrato.SANKHYA_PASSWORD || '',
      OAUTH_CLIENT_ID: contrato.OAUTH_CLIENT_ID || '',
      OAUTH_CLIENT_SECRET: contrato.OAUTH_CLIENT_SECRET || '',
      OAUTH_X_TOKEN: contrato.OAUTH_X_TOKEN || '',
      GEMINI_API_KEY: contrato.GEMINI_API_KEY || '',
      AI_PROVEDOR: contrato.AI_PROVEDOR || 'Gemini',
      AI_MODELO: contrato.AI_MODELO || 'gemini-2.0-flash',
      AI_CREDENTIAL: contrato.AI_CREDENTIAL || '',
      ATIVO: contrato.ATIVO,
      IS_SANDBOX: contrato.IS_SANDBOX !== undefined ? contrato.IS_SANDBOX : (contrato as any).IS_SANDBOX === 'S',
      LICENCAS: contrato.LICENCAS || 0
    })
    setIsEditing(true)
    setIsModalOpen(true)
  }

  const handleDelete = async (id: number) => {
    if (!confirm("Deseja realmente excluir este contrato?")) return

    try {
      const response = await fetch("/api/contratos/deletar", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ID_EMPRESA: id })
      })

      if (response.ok) {
        toast.success("Contrato excluído com sucesso")
        carregarContratos()
      } else {
        toast.error("Erro ao excluir contrato")
      }
    } catch (error) {
      toast.error("Erro ao excluir contrato")
    }
  }

  const resetForm = () => {
    setFormData(initialFormData)
    setIsEditing(false)
    setCurrentContrato(null)
    setEditingId(null)
  }

  const togglePasswordVisibility = (field: string) => {
    setShowPasswords(prev => ({ ...prev, [field]: !prev[field] }))
  }

  const formatCNPJ = (value: string) => {
    return value
      .replace(/\D/g, "")
      .replace(/^(\d{2})(\d)/, "$1.$2")
      .replace(/^(\d{2})\.(\d{3})(\d)/, "$1.$2.$3")
      .replace(/\.(\d{3})(\d)/, ".$1/$2")
      .replace(/(\d{4})(\d)/, "$1-$2")
      .slice(0, 18)
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Contratos</h1>
            <p className="text-muted-foreground">Gerencie os contratos e credenciais das empresas</p>
          </div>
          <Button onClick={() => { resetForm(); setIsModalOpen(true) }}>
            <Plus className="w-4 h-4 mr-2" />
            Novo Contrato
          </Button>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Contratos Cadastrados</CardTitle>
            <CardDescription>Lista de todas as empresas contratantes</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>ID</TableHead>
                  <TableHead>Empresa</TableHead>
                  <TableHead>CNPJ</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Ambiente</TableHead>
                  <TableHead>Tipo Autenticação</TableHead>
                  <TableHead className="text-center">Licenças</TableHead>
                  <TableHead className="text-right">Ações</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {contratos.map((contrato) => (
                  <TableRow key={contrato.ID_EMPRESA}>
                    <TableCell>{contrato.ID_EMPRESA}</TableCell>
                    <TableCell className="font-medium">{contrato.EMPRESA}</TableCell>
                    <TableCell>{contrato.CNPJ}</TableCell>
                    <TableCell>
                      <Badge variant={contrato.ATIVO ? "default" : "secondary"}>
                        {contrato.ATIVO ? "Ativo" : "Inativo"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={contrato.IS_SANDBOX ? "outline" : "default"}>
                        {contrato.IS_SANDBOX ? "Sandbox" : "Produção"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={contrato.AUTH_TYPE === 'OAUTH2' ? "default" : "secondary"}>
                        {contrato.AUTH_TYPE === 'OAUTH2' ? "OAuth 2.0" : "Legado"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center">
                      <Badge variant="secondary" className="font-semibold">
                        {contrato.LICENCAS || 0}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <Button variant="ghost" size="sm" onClick={() => handleEdit(contrato)}>
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm" onClick={() => handleDelete(contrato.ID_EMPRESA)}>
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {contratos.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center text-muted-foreground">
                      Nenhum contrato cadastrado
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
          <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>{isEditing ? "Editar Contrato" : "Novo Contrato"}</DialogTitle>
              <DialogDescription>
                Preencha as informações da empresa e suas credenciais
              </DialogDescription>
            </DialogHeader>

            <form onSubmit={handleSubmit} className="space-y-6">
              {/* Informações Básicas */}
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="empresa">Empresa *</Label>
                  <Input
                    id="empresa"
                    value={formData.EMPRESA}
                    onChange={(e) => setFormData({ ...formData, EMPRESA: e.target.value })}
                    required
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="cnpj">CNPJ *</Label>
                  <Input
                    id="cnpj"
                    value={formData.CNPJ}
                    onChange={(e) => setFormData({ ...formData, CNPJ: formatCNPJ(e.target.value) })}
                    required
                  />
                </div>
              </div>

              <div className="space-y-2 border rounded-lg p-4 bg-muted/30">
                <Label htmlFor="licencas" className="text-sm font-semibold flex items-center gap-2">
                  <Badge variant="secondary">Licenças</Badge>
                  Quantidade de Licenças Disponíveis
                </Label>
                <Input
                  id="licencas"
                  type="number"
                  min="0"
                  value={formData.LICENCAS || 0}
                  onChange={(e) => setFormData({ ...formData, LICENCAS: parseInt(e.target.value) || 0 })}
                  className="font-semibold text-lg"
                />
                <p className="text-xs text-muted-foreground">
                  Define quantas licenças estão disponíveis para uso no aplicativo externo
                </p>
              </div>

              {/* Configurações de IA */}
              <div className="border-t pt-4 space-y-4">
                <h3 className="text-sm font-semibold mb-3">Configurações de Inteligência Artificial</h3>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="ai_provedor">Provedor de IA</Label>
                    <select
                      id="ai_provedor"
                      className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                      value={formData.AI_PROVEDOR}
                      onChange={(e) => setFormData({ ...formData, AI_PROVEDOR: e.target.value })}
                    >
                      <option value="Gemini">Google Gemini</option>
                      <option value="OpenAI">OpenAI (ChatGPT)</option>
                      <option value="Grok">xAI (Grok)</option>
                    </select>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="ai_modelo">Modelo</Label>
                    <Input
                      id="ai_modelo"
                      placeholder="Ex: gemini-2.0-flash, gpt-4o"
                      value={formData.AI_MODELO}
                      onChange={(e) => setFormData({ ...formData, AI_MODELO: e.target.value })}
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="ai_credential">API Key / Credencial</Label>
                  <div className="relative">
                    <Input
                      id="ai_credential"
                      type={showPasswords.gemini ? "text" : "password"}
                      value={formData.AI_CREDENTIAL}
                      placeholder="Cole aqui a chave de API do provedor selecionado"
                      onChange={(e) => setFormData({ ...formData, AI_CREDENTIAL: e.target.value })}
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="absolute right-0 top-0 h-full"
                      onClick={() => togglePasswordVisibility("gemini")}
                    >
                      {showPasswords.gemini ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                    </Button>
                  </div>
                  <p className="text-[10px] text-muted-foreground italic">
                    * Se deixado em branco, o sistema tentará usar a chave Gemini padrão configurada anteriormente.
                  </p>
                </div>
              </div>

              <div className="flex items-center space-x-2 border-t pt-4">
                <Switch
                  id="ativo"
                  checked={formData.ATIVO}
                  onCheckedChange={(checked) => setFormData({ ...formData, ATIVO: checked })}
                />
                <Label htmlFor="ativo">Contrato Ativo</Label>
              </div>

              <div className="space-y-2 border-t pt-4">
                <Label htmlFor="isSandbox">Ambiente Sankhya</Label>
                <div className="flex items-center space-x-2">
                  <Switch
                    id="isSandbox"
                    checked={formData.IS_SANDBOX || false}
                    onCheckedChange={(checked) =>
                      setFormData({ ...formData, IS_SANDBOX: checked })
                    }
                  />
                  <Label htmlFor="isSandbox" className="font-normal">
                    {formData.IS_SANDBOX ? 'Sandbox' : 'Produção'}
                  </Label>
                </div>
                <p className="text-xs text-muted-foreground">
                  {formData.IS_SANDBOX
                    ? 'Usando: https://api.sandbox.sankhya.com.br'
                    : 'Usando: https://api.sankhya.com.br'}
                </p>
              </div>

              {/* Seletor de Tipo de Autenticação - MOVIDO PARA O FINAL */}
              <div className="space-y-2 border-t pt-4">
                <Label htmlFor="authType" className="text-sm font-semibold flex items-center gap-2">
                  <Badge variant="default">Tipo de Autenticação</Badge>
                  Método de Autenticação Sankhya
                </Label>
                <div className="flex items-center space-x-2">
                  <Switch
                    id="authType"
                    checked={formData.AUTH_TYPE === 'OAUTH2'}
                    onCheckedChange={(checked) =>
                      setFormData({ ...formData, AUTH_TYPE: checked ? 'OAUTH2' : 'LEGACY' })
                    }
                  />
                  <Label htmlFor="authType" className="font-normal">
                    {formData.AUTH_TYPE === 'OAUTH2' ? 'OAuth 2.0' : 'Autenticação Legada'}
                  </Label>
                </div>
                <p className="text-xs text-muted-foreground">
                  {formData.AUTH_TYPE === 'OAUTH2'
                    ? 'Usando client_id e client_secret para autenticação'
                    : 'Usando token, appkey, username e password para autenticação'}
                </p>
              </div>

              {/* Credenciais Sankhya - LEGADO */}
              {formData.AUTH_TYPE === 'LEGACY' && (
                <div className="border rounded-lg p-4 bg-muted/30">
                  <h3 className="text-sm font-semibold mb-3">Credenciais Sankhya (Legado)</h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="sankhya_token">Token</Label>
                      <div className="relative">
                        <Input
                          id="sankhya_token"
                          type={showPasswords.token ? "text" : "password"}
                          value={formData.SANKHYA_TOKEN}
                          onChange={(e) => setFormData({ ...formData, SANKHYA_TOKEN: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("token")}
                        >
                          {showPasswords.token ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="sankhya_appkey">App Key</Label>
                      <div className="relative">
                        <Input
                          id="sankhya_appkey"
                          type={showPasswords.appkey ? "text" : "password"}
                          value={formData.SANKHYA_APPKEY}
                          onChange={(e) => setFormData({ ...formData, SANKHYA_APPKEY: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("appkey")}
                        >
                          {showPasswords.appkey ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="sankhya_username">Username</Label>
                      <Input
                        id="sankhya_username"
                        value={formData.SANKHYA_USERNAME}
                        onChange={(e) => setFormData({ ...formData, SANKHYA_USERNAME: e.target.value })}
                      />
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="sankhya_password">Password</Label>
                      <div className="relative">
                        <Input
                          id="sankhya_password"
                          type={showPasswords.password ? "text" : "password"}
                          value={formData.SANKHYA_PASSWORD}
                          onChange={(e) => setFormData({ ...formData, SANKHYA_PASSWORD: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("password")}
                        >
                          {showPasswords.password ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Credenciais Sankhya - OAUTH 2.0 */}
              {formData.AUTH_TYPE === 'OAUTH2' && (
                <div className="border rounded-lg p-4 bg-muted/30">
                  <h3 className="text-sm font-semibold mb-3">Credenciais OAuth 2.0</h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="oauth_x_token">X-Token</Label>
                      <div className="relative">
                        <Input
                          id="oauth_x_token"
                          type={showPasswords.xToken ? "text" : "password"}
                          value={formData.OAUTH_X_TOKEN}
                          onChange={(e) => setFormData({ ...formData, OAUTH_X_TOKEN: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("xToken")}
                        >
                          {showPasswords.xToken ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="oauth_client_id">Client ID</Label>
                      <div className="relative">
                        <Input
                          id="oauth_client_id"
                          type={showPasswords.clientId ? "text" : "password"}
                          value={formData.OAUTH_CLIENT_ID}
                          onChange={(e) => setFormData({ ...formData, OAUTH_CLIENT_ID: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("clientId")}
                        >
                          {showPasswords.clientId ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>

                    <div className="space-y-2 col-span-2">
                      <Label htmlFor="oauth_client_secret">Client Secret</Label>
                      <div className="relative">
                        <Input
                          id="oauth_client_secret"
                          type={showPasswords.clientSecret ? "text" : "password"}
                          value={formData.OAUTH_CLIENT_SECRET}
                          onChange={(e) => setFormData({ ...formData, OAUTH_CLIENT_SECRET: e.target.value })}
                        />
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="absolute right-0 top-0 h-full"
                          onClick={() => togglePasswordVisibility("clientSecret")}
                        >
                          {showPasswords.clientSecret ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                        </Button>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              <DialogFooter>
                <Button type="button" variant="outline" onClick={() => setIsModalOpen(false)}>
                  Cancelar
                </Button>
                <Button type="submit">
                  {isEditing ? "Atualizar" : "Cadastrar"}
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}