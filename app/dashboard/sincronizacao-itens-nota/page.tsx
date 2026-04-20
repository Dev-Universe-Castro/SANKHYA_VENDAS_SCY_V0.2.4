"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/hooks/use-toast"
import { RefreshCw, Clock, Database, FileText, ListOrdered } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  ATIVO: boolean
  SYNC_ATIVO: boolean
}

interface EstatisticaSync {
  ID_SISTEMA: number
  TOTAL_REGISTROS: number
  REGISTROS_ATIVOS: number
  REGISTROS_DELETADOS: number
  ULTIMA_SINCRONIZACAO: string
}

interface SyncResult {
  success: boolean
  idSistema: number
  empresa: string
  totalRegistros: number
  registrosInseridos: number
  registrosAtualizados: number
  registrosDeletados: number
  dataInicio: string
  dataFim: string
  duracao: number
  erro?: string
}

export default function SincronizacaoItensNotaPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [estatisticas, setEstatisticas] = useState<Map<number, EstatisticaSync>>(new Map())
  const [loading, setLoading] = useState(true)
  const [syncingAll, setSyncingAll] = useState(false)
  const [syncingOne, setSyncingOne] = useState<number | null>(null)
  const [isPageDialogOpen, setIsPageDialogOpen] = useState(false)
  const [selectedContrato, setSelectedContrato] = useState<Contrato | null>(null)
  const [startPage, setStartPage] = useState<string>("0")

  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== 'Administrador') {
      router.push("/dashboard")
      return
    }
    loadData()
  }, [router])

  const loadData = async () => {
    try {
      setLoading(true)

      const contratosRes = await fetch('/api/contratos')
      if (contratosRes.ok) {
        const contratosData = await contratosRes.json()
        setContratos(contratosData.filter((c: Contrato) => c.ATIVO))
      }

      const statsRes = await fetch('/api/sync/itens-nota')
      if (statsRes.ok) {
        const statsData = await statsRes.json()
        const statsMap = new Map<number, EstatisticaSync>()
        statsData.forEach((stat: EstatisticaSync) => {
          statsMap.set(stat.ID_SISTEMA, stat)
        })
        setEstatisticas(statsMap)
      }
    } catch (error) {
      console.error('Erro ao carregar dados:', error)
      toast({
        title: "Erro",
        description: "Erro ao carregar dados de sincronização",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const sincronizarTodas = async () => {
    try {
      setSyncingAll(true)
      toast({
        title: "Sincronização iniciada",
        description: "Sincronizando itens de nota de todas as empresas..."
      })

      const response = await fetch('/api/sync/itens-nota', {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error('Erro ao sincronizar')
      }

      const resultados: SyncResult[] = await response.json()

      const sucessos = resultados.filter(r => r.success).length
      const falhas = resultados.filter(r => !r.success).length

      toast({
        title: "Sincronização concluída",
        description: `${sucessos} empresas sincronizadas com sucesso. ${falhas > 0 ? `${falhas} falhas.` : ''}`,
        variant: falhas > 0 ? "destructive" : "default"
      })

      await loadData()
    } catch (error) {
      console.error('Erro ao sincronizar:', error)
      toast({
        title: "Erro",
        description: "Erro ao sincronizar empresas",
        variant: "destructive"
      })
    } finally {
      setSyncingAll(false)
    }
  }

  const sincronizarEmpresa = async (idSistema: number, empresa: string, type: 'total' | 'partial' = 'total', startPage?: number) => {
    try {
      setSyncingOne(idSistema)
      toast({
        title: startPage !== undefined ? `Retomada iniciada` : `Sincronização ${type === 'partial' ? 'Parcial' : 'Total'} iniciada`,
        description: `Sincronizando itens de nota de ${empresa}${startPage !== undefined ? ` a partir da página ${startPage}` : ''}...`
      })

      const response = await fetch('/api/sync/itens-nota', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ idSistema, empresa, type, startPage }),
      })

      if (!response.ok) {
        throw new Error('Erro ao sincronizar')
      }

      const resultado: SyncResult = await response.json()

      if (resultado.success) {
        toast({
          title: "Sincronização concluída",
          description: `${resultado.totalRegistros} registros processados`,
        })
      } else {
        toast({
          title: "Erro na sincronização",
          description: resultado.erro || "Erro desconhecido",
          variant: "destructive"
        })
      }

      await loadData()
    } catch (error) {
      console.error('Erro ao sincronizar:', error)
      toast({
        title: "Erro",
        description: "Erro ao sincronizar empresa",
        variant: "destructive"
      })
    } finally {
      setSyncingOne(null)
    }
  }

  const formatarData = (dataStr: string) => {
    if (!dataStr) return 'Nunca sincronizado'
    const data = new Date(dataStr)
    return data.toLocaleString('pt-BR')
  }

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-96">
          <RefreshCw className="w-8 h-8 animate-spin text-primary" />
        </div>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6 p-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold">Sincronização de itens de nota</h1>
            <p className="text-muted-foreground mt-2">
              Gerencie a sincronização de itens de nota entre Sankhya e o sistema local
            </p>
          </div>
          <Button
            onClick={sincronizarTodas}
            disabled={syncingAll || syncingOne !== null}
            className="gap-2"
          >
            {syncingAll ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                Sincronizando...
              </>
            ) : (
              <>
                <RefreshCw className="w-4 h-4" />
                Sincronizar Todas
              </>
            )}
          </Button>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Empresas Cadastradas</CardTitle>
            <CardDescription>
              Status de sincronização por empresa
            </CardDescription>
          </CardHeader>
          <CardContent>
            {contratos.length === 0 ? (
              <div className="text-center py-12">
                <FileText className="w-16 h-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                <p className="text-lg font-medium">Nenhuma empresa ativa encontrada</p>
              </div>
            ) : (
              <div className="border rounded-lg overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>ID</TableHead>
                      <TableHead>Empresa</TableHead>
                      <TableHead>CNPJ</TableHead>
                      <TableHead className="text-center">Total</TableHead>
                      <TableHead className="text-center">Ativos</TableHead>
                      <TableHead>Última Sync</TableHead>
                      <TableHead className="text-right">Ações</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {contratos.map((contrato) => {
                      const stats = estatisticas.get(contrato.ID_EMPRESA)
                      const isSyncing = syncingOne === contrato.ID_EMPRESA

                      return (
                        <TableRow key={contrato.ID_EMPRESA}>
                          <TableCell>
                            <Badge variant="outline">{contrato.ID_EMPRESA}</Badge>
                          </TableCell>
                          <TableCell className="font-medium">{contrato.EMPRESA}</TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {contrato.CNPJ}
                          </TableCell>
                          <TableCell className="text-center">
                            {stats ? (
                              <div className="flex items-center justify-center gap-1">
                                <Database className="w-4 h-4 text-muted-foreground" />
                                <span className="font-semibold">{stats.TOTAL_REGISTROS}</span>
                              </div>
                            ) : (
                              <span className="text-muted-foreground">-</span>
                            )}
                          </TableCell>
                          <TableCell className="text-center">
                            {stats ? (
                              <span className="font-semibold text-green-600">
                                {stats.REGISTROS_ATIVOS}
                              </span>
                            ) : (
                              <span className="text-muted-foreground">-</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2 text-xs">
                              <Clock className="w-3 h-3 text-muted-foreground" />
                              <span className="text-muted-foreground">
                                {stats
                                  ? formatarData(stats.ULTIMA_SINCRONIZACAO)
                                  : 'Nunca sincronizado'}
                              </span>
                            </div>
                          </TableCell>
                          <TableCell className="text-right">
                            <div className="flex justify-end gap-2">
                              <Button
                                onClick={() => sincronizarEmpresa(contrato.ID_EMPRESA, contrato.EMPRESA, 'total')}
                                disabled={isSyncing || syncingAll || contrato.SYNC_ATIVO}
                                size="sm"
                                variant="outline"
                                title="Sincronização Total"
                              >
                                {isSyncing ? (
                                  <RefreshCw className="w-4 h-4 animate-spin" />
                                ) : (
                                  <>
                                    <RefreshCw className="w-4 h-4 mr-1" />
                                    Total
                                  </>
                                )}
                              </Button>
                              <Button
                                onClick={() => {
                                  setSelectedContrato(contrato)
                                  setIsPageDialogOpen(true)
                                }}
                                disabled={isSyncing || syncingAll || contrato.SYNC_ATIVO}
                                size="sm"
                                variant="outline"
                                className="bg-blue-50 hover:bg-blue-100 text-blue-700 border-blue-200"
                                title="Sincronização por Página"
                              >
                                {isSyncing ? (
                                  <RefreshCw className="w-4 h-4 animate-spin" />
                                ) : (
                                  <>
                                    <ListOrdered className="w-4 h-4 mr-1" />
                                    Paginação
                                  </>
                                )}
                              </Button>
                              <Button
                                onClick={() => sincronizarEmpresa(contrato.ID_EMPRESA, contrato.EMPRESA, 'partial')}
                                disabled={isSyncing || syncingAll || contrato.SYNC_ATIVO}
                                size="sm"
                                variant="secondary"
                                title="Sincronização Parcial"
                              >
                                {isSyncing ? (
                                  <RefreshCw className="w-4 h-4 animate-spin" />
                                ) : (
                                  <>
                                    <Clock className="w-4 h-4 mr-1" />
                                    Parcial
                                  </>
                                )}
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>

        <Dialog open={isPageDialogOpen} onOpenChange={setIsPageDialogOpen}>
          <DialogContent className="sm:max-w-[425px]">
            <DialogHeader>
              <DialogTitle>Sincronização por Página (Itens)</DialogTitle>
              <DialogDescription>
                Escolha a página inicial para a sincronização de itens de <strong>{selectedContrato?.EMPRESA}</strong>.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-4 py-4">
              <div className="grid grid-cols-4 items-center gap-4">
                <Label htmlFor="page" className="text-right">
                  Página Inicial
                </Label>
                <Input
                  id="page"
                  type="number"
                  value={startPage}
                  onChange={(e) => setStartPage(e.target.value)}
                  className="col-span-3"
                  min="0"
                />
              </div>
              <p className="text-xs text-muted-foreground bg-yellow-50 p-2 rounded border border-yellow-200">
                Atenção: A sincronização por página ignora o reset inicial (Soft Delete) e apenas atualiza/insere itens encontrados a partir da página informada.
              </p>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setIsPageDialogOpen(false)}>
                Cancelar
              </Button>
              <Button
                onClick={() => {
                  if (selectedContrato) {
                    sincronizarEmpresa(selectedContrato.ID_EMPRESA, selectedContrato.EMPRESA, 'total', Number(startPage))
                    setIsPageDialogOpen(false)
                  }
                }}
              >
                Iniciar Sincronização
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}