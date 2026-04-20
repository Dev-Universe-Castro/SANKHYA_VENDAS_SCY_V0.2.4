
"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  RefreshCw,
  CheckCircle,
  XCircle,
  Package,
  Database,
  Users,
  Clock,
  TrendingUp,
  AlertTriangle,
  FileKey,
  Filter
} from "lucide-react"
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
import { useRouter } from "next/navigation"

interface DashboardStats {
  totalContratos: number
  contratosAtivos: number
  totalLicencas: number
  totalSincronizacoes: number
  sincronizacoesSucesso: number
  sincronizacoesFalha: number
  ultimasSincronizacoes: any[]
  tabelasStats: any[]
}

export default function DashboardHome() {
  const router = useRouter()
  const [stats, setStats] = useState<DashboardStats>({
    totalContratos: 0,
    contratosAtivos: 0,
    totalLicencas: 0,
    totalSincronizacoes: 0,
    sincronizacoesSucesso: 0,
    sincronizacoesFalha: 0,
    ultimasSincronizacoes: [],
    tabelasStats: []
  })
  const [loading, setLoading] = useState(true)
  const [queueStatus, setQueueStatus] = useState<any>(null)
  const [contratos, setContratos] = useState<any[]>([])
  const [empresaSelecionada, setEmpresaSelecionada] = useState<string>("todas")

  useEffect(() => {
    loadDashboardData()
    const interval = setInterval(loadDashboardData, 30000) // Atualizar a cada 30 segundos
    return () => clearInterval(interval)
  }, [empresaSelecionada])

  const loadDashboardData = async () => {
    try {
      setLoading(true)

      // Buscar contratos
      const contratosRes = await fetch('/api/contratos')
      const contratosData = contratosRes.ok ? await contratosRes.json() : []
      setContratos(contratosData)

      // Aplicar filtro por empresa se selecionada
      const idEmpresaFiltro = empresaSelecionada !== "todas" ? parseInt(empresaSelecionada) : null

      // Buscar logs de sincronização com filtro
      const logsUrl = idEmpresaFiltro
        ? `/api/sync/logs?limit=10&idEmpresa=${idEmpresaFiltro}`
        : '/api/sync/logs?limit=10'
      const logsRes = await fetch(logsUrl)
      const logsData = logsRes.ok ? await logsRes.json() : { logs: [], total: 0 }

      // Buscar estatísticas gerais com filtro
      const statsUrl = idEmpresaFiltro
        ? `/api/sync/logs?action=stats&idEmpresa=${idEmpresaFiltro}`
        : '/api/sync/logs?action=stats'
      const statsRes = await fetch(statsUrl)
      const statsData = statsRes.ok ? await statsRes.json() : {
        totalSincronizacoes: 0,
        sucessos: 0,
        falhas: 0,
        porTabela: []
      }

      // Buscar status da fila
      const queueRes = await fetch('/api/sync/queue')
      const queueData = queueRes.ok ? await queueRes.json() : null

      // Filtrar contratos se necessário
      const contratosFiltrados = idEmpresaFiltro
        ? contratosData.filter((c: any) => c.ID_EMPRESA === idEmpresaFiltro)
        : contratosData

      setStats({
        totalContratos: contratosFiltrados.length,
        contratosAtivos: contratosFiltrados.filter((c: any) => c.ATIVO).length,
        totalLicencas: contratosFiltrados.reduce((sum: number, c: any) => sum + (c.LICENCAS || 0), 0),
        totalSincronizacoes: statsData.totalSincronizacoes,
        sincronizacoesSucesso: statsData.sucessos,
        sincronizacoesFalha: statsData.falhas,
        ultimasSincronizacoes: logsData.logs.slice(0, 10),
        tabelasStats: statsData.porTabela
      })

      setQueueStatus(queueData)
    } catch (error) {
      console.error('Erro ao carregar dados do dashboard:', error)
    } finally {
      setLoading(false)
    }
  }

  const formatarData = (dataStr: string) => {
    if (!dataStr) return '-'
    const data = new Date(dataStr)
    return data.toLocaleString('pt-BR')
  }

  const formatarDuracao = (ms: number) => {
    if (!ms) return '-'
    if (ms < 1000) return `${ms}ms`
    return `${(ms / 1000).toFixed(2)}s`
  }

  const taxaSucesso = stats.totalSincronizacoes > 0
    ? ((stats.sincronizacoesSucesso / stats.totalSincronizacoes) * 100).toFixed(1)
    : 0

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <RefreshCw className="w-8 h-8 animate-spin text-primary" />
      </div>
    )
  }

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard de Sincronizações</h1>
          <p className="text-muted-foreground mt-2">
            Visão geral do sistema de sincronização Sankhya
          </p>
        </div>
        <div className="flex gap-2 items-center">
          <Select value={empresaSelecionada} onValueChange={setEmpresaSelecionada}>
            <SelectTrigger className="w-[250px]">
              <Filter className="w-4 h-4 mr-2" />
              <SelectValue placeholder="Filtrar por empresa" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todas">Todas as Empresas</SelectItem>
              {contratos.map((contrato) => (
                <SelectItem key={contrato.ID_EMPRESA} value={contrato.ID_EMPRESA.toString()}>
                  {contrato.EMPRESA}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button onClick={loadDashboardData} variant="outline" size="sm">
            <RefreshCw className="w-4 h-4 mr-2" />
            Atualizar
          </Button>
        </div>
      </div>

      {/* Cards de Métricas Principais */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Contratos Ativos</CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats.contratosAtivos}</div>
            <p className="text-xs text-muted-foreground">
              {stats.totalContratos} total
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Licenças Disponíveis</CardTitle>
            <FileKey className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats.totalLicencas}</div>
            <p className="text-xs text-muted-foreground">
              Total de licenças configuradas
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Taxa de Sucesso</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{taxaSucesso}%</div>
            <p className="text-xs text-muted-foreground">
              {stats.sincronizacoesSucesso} sucessos
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Falhas</CardTitle>
            <AlertTriangle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-destructive">{stats.sincronizacoesFalha}</div>
            <p className="text-xs text-muted-foreground">
              Total de erros
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Status da Fila */}
      {queueStatus && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5" />
              Status da Fila de Sincronização
            </CardTitle>
            <CardDescription>
              Acompanhamento em tempo real das sincronizações
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-6 md:grid-cols-3 mb-6">
              <div className="flex flex-col gap-1">
                <span className="text-sm font-medium text-muted-foreground">Status Geral</span>
                <Badge variant={queueStatus.isProcessing ? "default" : "secondary"} className="w-fit">
                  {queueStatus.isProcessing ? "Processando" : "Aguardando"}
                </Badge>
              </div>
              <div className="flex flex-col gap-1">
                <span className="text-sm font-medium text-muted-foreground">Em Processamento</span>
                <span className="text-2xl font-bold">{queueStatus.contractsInProcessing?.length || 0}</span>
              </div>
              <div className="flex flex-col gap-1">
                <span className="text-sm font-medium text-muted-foreground">Na Fila de Espera</span>
                <span className="text-2xl font-bold">{queueStatus.queueLength}</span>
              </div>
            </div>

            <div className="grid gap-6 md:grid-cols-2">
              {/* Em Processamento */}
              <div className="border rounded-md p-4 bg-muted/20">
                <h3 className="font-semibold mb-3 flex items-center gap-2">
                  <RefreshCw className="w-4 h-4 animate-spin text-blue-500" />
                  Em Andamento
                </h3>
                {(queueStatus.contractsInProcessing && queueStatus.contractsInProcessing.length > 0) ? (
                  <ul className="space-y-2">
                    {queueStatus.contractsInProcessing.map((id: number) => {
                      const contrato = contratos.find((c: any) => c.ID_EMPRESA === id);
                      return (
                        <li key={id} className="text-sm bg-background p-2 rounded border shadow-sm flex justify-between items-center">
                          <span className="font-medium truncate mr-2">
                            {contrato ? contrato.EMPRESA : `Empresa ID: ${id}`}
                          </span>
                          <Badge variant="outline" className="text-xs">Processando</Badge>
                        </li>
                      );
                    })}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground italic">Nenhuma sincronização ativa no momento.</p>
                )}
              </div>

              {/* Na Fila */}
              <div className="border rounded-md p-4 bg-muted/20">
                <h3 className="font-semibold mb-3 flex items-center gap-2">
                  <Clock className="w-4 h-4 text-orange-500" />
                  Próximos na Fila
                </h3>
                {(queueStatus.queue && queueStatus.queue.length > 0) ? (
                  <ul className="space-y-2 max-h-[200px] overflow-y-auto pr-2">
                    {queueStatus.queue.map((item: any, index: number) => (
                      <li key={index} className="text-sm bg-background p-2 rounded border shadow-sm flex justify-between items-center">
                        <span className="font-medium truncate mr-2">{item.empresa}</span>
                        <span className="text-xs text-muted-foreground">
                          {new Date(item.timestamp).toLocaleTimeString()}
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground italic">Fila de espera vazia.</p>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        {/* Estatísticas por Tabela */}
        <Card>
          <CardHeader>
            <CardTitle>Sincronizações por Tabela</CardTitle>
            <CardDescription>Desempenho por tipo de dados</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Tabela</TableHead>
                  <TableHead className="text-center">Total</TableHead>
                  <TableHead className="text-center">Sucesso</TableHead>
                  <TableHead className="text-center">Falhas</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {stats.tabelasStats.slice(0, 8).map((tabela: any) => (
                  <TableRow key={tabela.tabela}>
                    <TableCell className="font-medium">{tabela.tabela}</TableCell>
                    <TableCell className="text-center">{tabela.total}</TableCell>
                    <TableCell className="text-center">
                      <div className="flex items-center justify-center gap-1">
                        <CheckCircle className="w-3 h-3 text-green-600" />
                        <span className="text-green-600">{tabela.sucessos}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-center">
                      <div className="flex items-center justify-center gap-1">
                        <XCircle className="w-3 h-3 text-red-600" />
                        <span className="text-red-600">{tabela.falhas}</span>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
                {stats.tabelasStats.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center text-muted-foreground">
                      Nenhuma sincronização registrada
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Últimas Sincronizações */}
        <Card>
          <CardHeader>
            <CardTitle>Últimas Sincronizações</CardTitle>
            <CardDescription>Histórico recente de operações</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Empresa</TableHead>
                  <TableHead>Tabela</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Duração</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {stats.ultimasSincronizacoes.map((log: any) => (
                  <TableRow key={log.ID_LOG}>
                    <TableCell className="font-medium">{log.EMPRESA}</TableCell>
                    <TableCell className="text-sm">{log.TABELA}</TableCell>
                    <TableCell>
                      <Badge variant={log.STATUS === 'SUCESSO' ? 'default' : 'destructive'}>
                        {log.STATUS}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right text-sm">
                      {formatarDuracao(log.DURACAO_MS)}
                    </TableCell>
                  </TableRow>
                ))}
                {stats.ultimasSincronizacoes.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center text-muted-foreground">
                      Nenhuma sincronização registrada
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>

      {/* Ações Rápidas */}
      <Card>
        <CardHeader>
          <CardTitle>Ações Rápidas</CardTitle>
          <CardDescription>Navegue para as principais áreas do sistema</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-3 lg:grid-cols-5">
            <Button
              variant="outline"
              className="justify-start"
              onClick={() => router.push('/dashboard/contratos')}
            >
              <Package className="w-4 h-4 mr-2" />
              Contratos
            </Button>
            <Button
              variant="outline"
              className="justify-start"
              onClick={() => router.push('/dashboard/logs-sincronizacao')}
            >
              <Database className="w-4 h-4 mr-2" />
              Logs
            </Button>
            <Button
              variant="outline"
              className="justify-start"
              onClick={() => router.push('/dashboard/sincronizacao')}
            >
              <RefreshCw className="w-4 h-4 mr-2" />
              Parceiros
            </Button>
            <Button
              variant="outline"
              className="justify-start"
              onClick={() => router.push('/dashboard/sincronizacao-produtos')}
            >
              <Package className="w-4 h-4 mr-2" />
              Produtos
            </Button>
            <Button
              variant="outline"
              className="justify-start"
              onClick={() => router.push('/dashboard/usuarios-fdv')}
            >
              <Users className="w-4 h-4 mr-2" />
              Usuários FDV
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
