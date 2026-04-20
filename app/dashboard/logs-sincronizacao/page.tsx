
"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/hooks/use-toast"
import { RefreshCw, CheckCircle, XCircle, Filter, Download, Calendar } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
  ATIVO: boolean
}

interface SyncLog {
  ID_LOG: number
  ID_SISTEMA: number
  EMPRESA: string
  TABELA: string
  STATUS: 'SUCESSO' | 'FALHA'
  TOTAL_REGISTROS: number
  REGISTROS_INSERIDOS: number
  REGISTROS_ATUALIZADOS: number
  REGISTROS_DELETADOS: number
  DURACAO_MS: number
  MENSAGEM_ERRO?: string
  DATA_INICIO: string
  DATA_FIM: string
  DATA_CRIACAO: string
}

interface LogStats {
  totalSincronizacoes: number
  sucessos: number
  falhas: number
  porTabela: { tabela: string; total: number; sucessos: number; falhas: number }[]
}

const TABELAS_DISPONIVEIS = [
  'AS_PARCEIROS',
  'AS_PRODUTOS',
  'AS_TIPOS_NEGOCIACAO',
  'AS_TIPOS_OPERACAO',
  'AS_ESTOQUES',
  'AS_TABELA_PRECOS',
  'AS_EXCECAO_PRECO',
  'AS_FINANCEIRO',
  'AS_VENDEDORES'
]

export default function LogsSincronizacaoPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [logs, setLogs] = useState<SyncLog[]>([])
  const [stats, setStats] = useState<LogStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [totalLogs, setTotalLogs] = useState(0)
  const [currentPage, setCurrentPage] = useState(1)
  const logsPerPage = 50

  // Filtros
  const [selectedEmpresa, setSelectedEmpresa] = useState<string>('TODOS')
  const [selectedTabela, setSelectedTabela] = useState<string>('TODOS')
  const [selectedStatus, setSelectedStatus] = useState<string>('TODOS')
  const [dataInicio, setDataInicio] = useState<string>('')
  const [dataFim, setDataFim] = useState<string>('')

  // Modal de detalhes
  const [selectedLog, setSelectedLog] = useState<SyncLog | null>(null)
  const [detailsModalOpen, setDetailsModalOpen] = useState(false)

  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== 'Administrador') {
      router.push("/dashboard")
      return
    }
    loadData()
  }, [router])

  useEffect(() => {
    if (!loading && contratos.length > 0) {
      loadLogs()
      loadStats()
    }
  }, [currentPage])

  const loadData = async () => {
    try {
      setLoading(true)
      
      const contratosRes = await fetch('/api/contratos')
      if (contratosRes.ok) {
        const contratosData = await contratosRes.json()
        setContratos(contratosData.filter((c: Contrato) => c.ATIVO))
      }

      await Promise.all([loadStats(), loadLogs()])
    } catch (error) {
      console.error('Erro ao carregar dados:', error)
      toast({
        title: "Erro",
        description: "Erro ao carregar dados de logs",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  const loadStats = async () => {
    try {
      const params = new URLSearchParams({ action: 'stats' })
      
      if (selectedEmpresa && selectedEmpresa !== 'TODOS') {
        const contrato = contratos.find(c => c.EMPRESA === selectedEmpresa)
        if (contrato) params.append('idSistema', String(contrato.ID_EMPRESA))
      }
      if (dataInicio) params.append('dataInicio', dataInicio)
      if (dataFim) params.append('dataFim', dataFim)

      const response = await fetch(`/api/sync/logs?${params}`)
      if (response.ok) {
        const data = await response.json()
        setStats(data)
      }
    } catch (error) {
      console.error('Erro ao carregar estatísticas:', error)
    }
  }

  const loadLogs = async () => {
    try {
      const params = new URLSearchParams({
        limit: String(logsPerPage),
        offset: String((currentPage - 1) * logsPerPage)
      })

      if (selectedEmpresa && selectedEmpresa !== 'TODOS') {
        const contrato = contratos.find(c => c.EMPRESA === selectedEmpresa)
        if (contrato) params.append('idSistema', String(contrato.ID_EMPRESA))
      }
      if (selectedTabela && selectedTabela !== 'TODOS') params.append('tabela', selectedTabela)
      if (selectedStatus && selectedStatus !== 'TODOS') params.append('status', selectedStatus)
      if (dataInicio) params.append('dataInicio', dataInicio)
      if (dataFim) params.append('dataFim', dataFim)

      const response = await fetch(`/api/sync/logs?${params}`)
      if (response.ok) {
        const data = await response.json()
        setLogs(data.logs)
        setTotalLogs(data.total)
      }
    } catch (error) {
      console.error('Erro ao carregar logs:', error)
      toast({
        title: "Erro",
        description: "Erro ao carregar logs",
        variant: "destructive"
      })
    }
  }

  const handleFilter = () => {
    setCurrentPage(1)
    loadLogs()
    loadStats()
  }

  const handleClearFilters = () => {
    setSelectedEmpresa('TODOS')
    setSelectedTabela('TODOS')
    setSelectedStatus('TODOS')
    setDataInicio('')
    setDataFim('')
    setCurrentPage(1)
  }

  const handleExportarXLS = async () => {
    try {
      const params = new URLSearchParams({ action: 'export' })

      if (selectedEmpresa && selectedEmpresa !== 'TODOS') {
        const contrato = contratos.find(c => c.EMPRESA === selectedEmpresa)
        if (contrato) params.append('idSistema', String(contrato.ID_EMPRESA))
      }
      if (selectedTabela && selectedTabela !== 'TODOS') params.append('tabela', selectedTabela)
      if (selectedStatus && selectedStatus !== 'TODOS') params.append('status', selectedStatus)
      if (dataInicio) params.append('dataInicio', dataInicio)
      if (dataFim) params.append('dataFim', dataFim)

      const response = await fetch(`/api/sync/logs?${params}`)
      if (response.ok) {
        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `logs-sincronizacao-${new Date().toISOString().split('T')[0]}.csv`
        document.body.appendChild(a)
        a.click()
        window.URL.revokeObjectURL(url)
        document.body.removeChild(a)

        toast({
          title: "Sucesso",
          description: "Logs exportados com sucesso"
        })
      } else {
        throw new Error('Erro ao exportar logs')
      }
    } catch (error) {
      console.error('Erro ao exportar logs:', error)
      toast({
        title: "Erro",
        description: "Erro ao exportar logs",
        variant: "destructive"
      })
    }
  }

  const openDetailsModal = (log: SyncLog) => {
    setSelectedLog(log)
    setDetailsModalOpen(true)
  }

  const formatarData = (dataStr: string) => {
    if (!dataStr) return '-'
    const data = new Date(dataStr)
    return data.toLocaleString('pt-BR')
  }

  const formatarDuracao = (ms: number) => {
    if (ms < 1000) return `${ms}ms`
    return `${(ms / 1000).toFixed(2)}s`
  }

  const totalPages = Math.ceil(totalLogs / logsPerPage)

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
            <h1 className="text-3xl font-bold">Logs de Sincronização</h1>
            <p className="text-muted-foreground mt-2">
              Visualize e analise os logs de sincronização do sistema
            </p>
          </div>
          <div className="flex gap-2">
            <Button onClick={handleExportarXLS} variant="outline" className="gap-2">
              <Download className="w-4 h-4" />
              Exportar XLS
            </Button>
            <Button onClick={() => { loadLogs(); loadStats(); }} className="gap-2">
              <RefreshCw className="w-4 h-4" />
              Atualizar
            </Button>
          </div>
        </div>

        {/* Estatísticas */}
        {stats && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card>
              <CardHeader className="pb-3">
                <CardDescription>Total de Sincronizações</CardDescription>
                <CardTitle className="text-3xl">{stats.totalSincronizacoes}</CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader className="pb-3">
                <CardDescription>Sucessos</CardDescription>
                <CardTitle className="text-3xl text-green-600">{stats.sucessos}</CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader className="pb-3">
                <CardDescription>Falhas</CardDescription>
                <CardTitle className="text-3xl text-red-600">{stats.falhas}</CardTitle>
              </CardHeader>
            </Card>
          </div>
        )}

        {/* Filtros */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Filter className="w-5 h-5" />
              Filtros
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
              <div className="space-y-2">
                <Label>Empresa</Label>
                <Select value={selectedEmpresa} onValueChange={setSelectedEmpresa}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="TODOS">Todas</SelectItem>
                    {contratos.map((c) => (
                      <SelectItem key={c.ID_EMPRESA} value={c.EMPRESA}>
                        {c.EMPRESA}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Tabela</Label>
                <Select value={selectedTabela} onValueChange={setSelectedTabela}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="TODOS">Todas</SelectItem>
                    {TABELAS_DISPONIVEIS.map((t) => (
                      <SelectItem key={t} value={t}>
                        {t}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Status</Label>
                <Select value={selectedStatus} onValueChange={setSelectedStatus}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todos" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="TODOS">Todos</SelectItem>
                    <SelectItem value="SUCESSO">Sucesso</SelectItem>
                    <SelectItem value="FALHA">Falha</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Data Início</Label>
                <Input
                  type="date"
                  value={dataInicio}
                  onChange={(e) => setDataInicio(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <Label>Data Fim</Label>
                <Input
                  type="date"
                  value={dataFim}
                  onChange={(e) => setDataFim(e.target.value)}
                />
              </div>
            </div>

            <div className="flex gap-2 mt-4">
              <Button onClick={handleFilter}>
                Aplicar Filtros
              </Button>
              <Button onClick={handleClearFilters} variant="outline">
                Limpar Filtros
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Tabela de Logs */}
        <Card>
          <CardHeader>
            <CardTitle>Logs de Sincronização ({totalLogs} registros)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="border rounded-lg overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Data</TableHead>
                    <TableHead>Empresa</TableHead>
                    <TableHead>Tabela</TableHead>
                    <TableHead className="text-center">Status</TableHead>
                    <TableHead className="text-center">Registros</TableHead>
                    <TableHead className="text-center">Inseridos</TableHead>
                    <TableHead className="text-center">Atualizados</TableHead>
                    <TableHead className="text-center">Deletados</TableHead>
                    <TableHead>Duração</TableHead>
                    <TableHead className="text-right">Ações</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {logs.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={10} className="text-center py-8 text-muted-foreground">
                        Nenhum log encontrado
                      </TableCell>
                    </TableRow>
                  ) : (
                    logs.map((log) => (
                      <TableRow key={log.ID_LOG}>
                        <TableCell className="text-sm">
                          {formatarData(log.DATA_CRIACAO)}
                        </TableCell>
                        <TableCell className="font-medium">{log.EMPRESA}</TableCell>
                        <TableCell>
                          <code className="text-xs bg-muted px-2 py-1 rounded">
                            {log.TABELA}
                          </code>
                        </TableCell>
                        <TableCell className="text-center">
                          {log.STATUS === 'SUCESSO' ? (
                            <Badge className="gap-1 bg-green-100 text-green-800 hover:bg-green-100">
                              <CheckCircle className="w-3 h-3" />
                              Sucesso
                            </Badge>
                          ) : (
                            <Badge variant="destructive" className="gap-1">
                              <XCircle className="w-3 h-3" />
                              Falha
                            </Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-center">{log.TOTAL_REGISTROS}</TableCell>
                        <TableCell className="text-center text-green-600">
                          {log.REGISTROS_INSERIDOS}
                        </TableCell>
                        <TableCell className="text-center text-blue-600">
                          {log.REGISTROS_ATUALIZADOS}
                        </TableCell>
                        <TableCell className="text-center text-orange-600">
                          {log.REGISTROS_DELETADOS}
                        </TableCell>
                        <TableCell className="text-sm">
                          {formatarDuracao(log.DURACAO_MS)}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            onClick={() => openDetailsModal(log)}
                            size="sm"
                            variant="outline"
                          >
                            Detalhes
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>

            {/* Paginação */}
            {totalPages > 1 && (
              <div className="flex justify-between items-center mt-4">
                <p className="text-sm text-muted-foreground">
                  Página {currentPage} de {totalPages}
                </p>
                <div className="flex gap-2">
                  <Button
                    onClick={() => setCurrentPage(currentPage - 1)}
                    disabled={currentPage === 1}
                    variant="outline"
                    size="sm"
                  >
                    Anterior
                  </Button>
                  <Button
                    onClick={() => setCurrentPage(currentPage + 1)}
                    disabled={currentPage === totalPages}
                    variant="outline"
                    size="sm"
                  >
                    Próxima
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Modal de Detalhes */}
        <Dialog open={detailsModalOpen} onOpenChange={setDetailsModalOpen}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>Detalhes da Sincronização</DialogTitle>
              <DialogDescription>Log ID: {selectedLog?.ID_LOG}</DialogDescription>
            </DialogHeader>
            
            {selectedLog && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label className="text-xs text-muted-foreground">Empresa</Label>
                    <p className="font-medium">{selectedLog.EMPRESA}</p>
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground">Tabela</Label>
                    <p className="font-medium">{selectedLog.TABELA}</p>
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground">Status</Label>
                    <p>
                      {selectedLog.STATUS === 'SUCESSO' ? (
                        <Badge className="bg-green-100 text-green-800">Sucesso</Badge>
                      ) : (
                        <Badge variant="destructive">Falha</Badge>
                      )}
                    </p>
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground">Duração</Label>
                    <p className="font-medium">{formatarDuracao(selectedLog.DURACAO_MS)}</p>
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground">Data Início</Label>
                    <p className="text-sm">{formatarData(selectedLog.DATA_INICIO)}</p>
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground">Data Fim</Label>
                    <p className="text-sm">{formatarData(selectedLog.DATA_FIM)}</p>
                  </div>
                </div>

                <div className="border-t pt-4">
                  <Label className="text-xs text-muted-foreground">Resumo de Registros</Label>
                  <div className="grid grid-cols-4 gap-4 mt-2">
                    <div className="text-center p-3 bg-muted rounded-lg">
                      <p className="text-2xl font-bold">{selectedLog.TOTAL_REGISTROS}</p>
                      <p className="text-xs text-muted-foreground">Total</p>
                    </div>
                    <div className="text-center p-3 bg-green-50 rounded-lg">
                      <p className="text-2xl font-bold text-green-600">
                        {selectedLog.REGISTROS_INSERIDOS}
                      </p>
                      <p className="text-xs text-muted-foreground">Inseridos</p>
                    </div>
                    <div className="text-center p-3 bg-blue-50 rounded-lg">
                      <p className="text-2xl font-bold text-blue-600">
                        {selectedLog.REGISTROS_ATUALIZADOS}
                      </p>
                      <p className="text-xs text-muted-foreground">Atualizados</p>
                    </div>
                    <div className="text-center p-3 bg-orange-50 rounded-lg">
                      <p className="text-2xl font-bold text-orange-600">
                        {selectedLog.REGISTROS_DELETADOS}
                      </p>
                      <p className="text-xs text-muted-foreground">Deletados</p>
                    </div>
                  </div>
                </div>

                {selectedLog.MENSAGEM_ERRO && (
                  <div className="border-t pt-4">
                    <Label className="text-xs text-muted-foreground">Mensagem de Erro</Label>
                    <div className="mt-2 p-3 bg-red-50 border border-red-200 rounded-lg">
                      <p className="text-sm text-red-800 font-mono whitespace-pre-wrap">
                        {selectedLog.MENSAGEM_ERRO}
                      </p>
                    </div>
                  </div>
                )}
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}
