
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
import { Badge } from "@/components/ui/badge"
import { Switch } from "@/components/ui/switch"
import { toast } from "sonner"
import { Clock, Save, Calendar } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  ATIVO: boolean
  SYNC_ATIVO?: boolean
  SYNC_INTERVALO_MINUTOS?: number
  ULTIMA_SINCRONIZACAO?: string
  PROXIMA_SINCRONIZACAO?: string
}

interface AgendamentoConfig {
  syncAtivo: boolean
  intervaloMinutos: number
  unidade: 'minutos' | 'horas' | 'dias'
  valor: number
}

export default function AgendamentosPage() {
  const router = useRouter()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [loading, setLoading] = useState(true)
  const [configs, setConfigs] = useState<Map<number, AgendamentoConfig>>(new Map())
  const [saving, setSaving] = useState<number | null>(null)

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
      setLoading(true)
      const response = await fetch("/api/contratos")
      if (response.ok) {
        const data = await response.json()
        const contratosAtivos = data.filter((c: Contrato) => c.ATIVO)
        setContratos(contratosAtivos)
        
        // Inicializar configurações
        const initialConfigs = new Map<number, AgendamentoConfig>()
        contratosAtivos.forEach((c: Contrato) => {
          const minutos = c.SYNC_INTERVALO_MINUTOS || 120
          const config = minutosParaConfig(minutos)
          initialConfigs.set(c.ID_EMPRESA, {
            syncAtivo: c.SYNC_ATIVO || false,
            intervaloMinutos: minutos,
            ...config
          })
        })
        setConfigs(initialConfigs)
      }
    } catch (error) {
      console.error("Erro ao carregar contratos:", error)
      toast.error("Erro ao carregar contratos")
    } finally {
      setLoading(false)
    }
  }

  const minutosParaConfig = (minutos: number) => {
    if (minutos >= 1440 && minutos % 1440 === 0) {
      return { unidade: 'dias' as const, valor: minutos / 1440 }
    } else if (minutos >= 60 && minutos % 60 === 0) {
      return { unidade: 'horas' as const, valor: minutos / 60 }
    }
    return { unidade: 'minutos' as const, valor: minutos }
  }

  const configParaMinutos = (valor: number, unidade: string) => {
    switch (unidade) {
      case 'minutos': return valor
      case 'horas': return valor * 60
      case 'dias': return valor * 1440
      default: return valor
    }
  }

  const handleConfigChange = (idEmpresa: number, field: string, value: any) => {
    const config = configs.get(idEmpresa)
    if (!config) return

    const newConfig = { ...config, [field]: value }
    
    if (field === 'valor' || field === 'unidade') {
      newConfig.intervaloMinutos = configParaMinutos(
        newConfig.valor,
        newConfig.unidade
      )
    }

    setConfigs(new Map(configs.set(idEmpresa, newConfig)))
  }

  const handleSave = async (idEmpresa: number) => {
    const config = configs.get(idEmpresa)
    if (!config) return

    try {
      setSaving(idEmpresa)
      
      const response = await fetch("/api/contratos/agendamento", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: idEmpresa,
          syncAtivo: config.syncAtivo,
          intervaloMinutos: config.intervaloMinutos
        })
      })

      if (response.ok) {
        toast.success("Agendamento salvo com sucesso")
        await carregarContratos()
      } else {
        const error = await response.json()
        toast.error(error.error || "Erro ao salvar agendamento")
      }
    } catch (error) {
      toast.error("Erro ao salvar agendamento")
    } finally {
      setSaving(null)
    }
  }

  const formatarData = (data?: string) => {
    if (!data) return 'Nunca'
    return new Date(data).toLocaleString('pt-BR')
  }

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-96">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary mx-auto mb-4"></div>
            <p className="text-muted-foreground">Carregando...</p>
          </div>
        </div>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Agendamentos de Sincronização</h1>
          <p className="text-muted-foreground">
            Configure a sincronização automática para cada empresa
          </p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Configurações de Agendamento</CardTitle>
            <CardDescription>
              Defina intervalos personalizados para sincronização automática de cada contrato
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Empresa</TableHead>
                  <TableHead className="text-center">Sync Ativo</TableHead>
                  <TableHead>Intervalo</TableHead>
                  <TableHead>Última Sync</TableHead>
                  <TableHead>Próxima Sync</TableHead>
                  <TableHead className="text-right">Ações</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {contratos.map((contrato) => {
                  const config = configs.get(contrato.ID_EMPRESA)
                  if (!config) return null

                  return (
                    <TableRow key={contrato.ID_EMPRESA}>
                      <TableCell>
                        <div>
                          <div className="font-medium">{contrato.EMPRESA}</div>
                          <div className="text-sm text-muted-foreground">{contrato.CNPJ}</div>
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        <Switch
                          checked={config.syncAtivo}
                          onCheckedChange={(checked) => 
                            handleConfigChange(contrato.ID_EMPRESA, 'syncAtivo', checked)
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Input
                            type="number"
                            min="1"
                            value={config.valor}
                            onChange={(e) => 
                              handleConfigChange(contrato.ID_EMPRESA, 'valor', parseInt(e.target.value) || 1)
                            }
                            className="w-20"
                            disabled={!config.syncAtivo}
                          />
                          <Select
                            value={config.unidade}
                            onValueChange={(value) => 
                              handleConfigChange(contrato.ID_EMPRESA, 'unidade', value)
                            }
                            disabled={!config.syncAtivo}
                          >
                            <SelectTrigger className="w-32">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="minutos">Minutos</SelectItem>
                              <SelectItem value="horas">Horas</SelectItem>
                              <SelectItem value="dias">Dias</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2 text-sm">
                          <Clock className="w-4 h-4 text-muted-foreground" />
                          <span className="text-muted-foreground">
                            {formatarData(contrato.ULTIMA_SINCRONIZACAO)}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2 text-sm">
                          <Calendar className="w-4 h-4 text-muted-foreground" />
                          <span className="text-muted-foreground">
                            {config.syncAtivo ? formatarData(contrato.PROXIMA_SINCRONIZACAO) : 'Desativado'}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          size="sm"
                          onClick={() => handleSave(contrato.ID_EMPRESA)}
                          disabled={saving === contrato.ID_EMPRESA}
                        >
                          {saving === contrato.ID_EMPRESA ? (
                            <>
                              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2" />
                              Salvando...
                            </>
                          ) : (
                            <>
                              <Save className="w-4 h-4 mr-2" />
                              Salvar
                            </>
                          )}
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })}
                {contratos.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground">
                      Nenhum contrato ativo encontrado
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Como Funciona</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <p>• As sincronizações são executadas automaticamente no intervalo configurado</p>
            <p>• Todas as tabelas (Parceiros, Produtos, Estoques, etc.) são sincronizadas consecutivamente</p>
            <p>• Se múltiplas empresas estiverem agendadas para o mesmo horário, elas serão processadas em fila</p>
            <p>• O sistema processa uma empresa por vez para evitar sobrecarga</p>
            <p>• Você pode desativar a sincronização automática a qualquer momento</p>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  )
}
