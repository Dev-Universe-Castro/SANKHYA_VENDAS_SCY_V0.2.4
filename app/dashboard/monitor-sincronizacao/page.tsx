
"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { RefreshCw, CheckCircle, Clock, Database, Play, Pause } from "lucide-react"
import { Progress } from "@/components/ui/progress"

interface QueueItem {
  idEmpresa: number
  empresa: string
  timestamp: string
}

interface QueueStatus {
  queueLength: number
  isProcessing: boolean
  isSyncRunning: boolean
  contractsInProcessing: number[]
  queue: QueueItem[]
}

interface SyncProgress {
  empresa: string
  tabela: string
  status: 'aguardando' | 'processando' | 'concluido' | 'erro'
  paginaAtual?: number
  totalPaginas?: number
  registrosProcessados?: number
  totalRegistros?: number
  fase?: string
  mensagem?: string
}

const TABELAS = [
  'Parceiros',
  'Produtos',
  'Tipos de Negociação',
  'Tipos de Operação',
  'Estoques',
  'Tabelas de Preços',
  'Exceção de Preço',
  'Vendedores',
  'Complemento Parceiro'
]

export default function MonitorSincronizacaoPage() {
  const router = useRouter()
  const [queueStatus, setQueueStatus] = useState<QueueStatus | null>(null)
  const [syncProgress, setSyncProgress] = useState<Map<string, SyncProgress>>(new Map())
  const [loading, setLoading] = useState(true)
  const [autoRefresh, setAutoRefresh] = useState(true)

  useEffect(() => {
    const currentUser = authService.getCurrentUser()
    if (!currentUser || currentUser.role !== 'Administrador') {
      router.push("/dashboard")
      return
    }
    loadData()
  }, [router])

  useEffect(() => {
    if (!autoRefresh) return

    const interval = setInterval(() => {
      loadData()
    }, 2000) // Atualiza a cada 2 segundos

    return () => clearInterval(interval)
  }, [autoRefresh])

  const loadData = async () => {
    try {
      const response = await fetch('/api/sync/queue')
      if (response.ok) {
        const data = await response.json()
        setQueueStatus(data)
        
        // Simular progresso baseado no status atual
        // Em uma implementação real, você teria uma API que retorna o progresso real
        updateSyncProgress(data)
      }
    } catch (error) {
      console.error('Erro ao carregar status da fila:', error)
    } finally {
      setLoading(false)
    }
  }

  const updateSyncProgress = (status: QueueStatus) => {
    const newProgress = new Map<string, SyncProgress>()

    // Empresas em processamento
    status.contractsInProcessing.forEach((idEmpresa) => {
      const queueItem = status.queue.find(q => q.idEmpresa === idEmpresa)
      if (queueItem) {
        TABELAS.forEach((tabela, index) => {
          const key = `${idEmpresa}-${tabela}`
          newProgress.set(key, {
            empresa: queueItem.empresa,
            tabela,
            status: index === 0 ? 'processando' : 'aguardando',
            fase: index === 0 ? 'Buscando dados do Sankhya...' : 'Aguardando processamento',
            paginaAtual: index === 0 ? 1 : 0,
            totalPaginas: index === 0 ? 10 : 0,
            registrosProcessados: index === 0 ? 50 : 0,
            totalRegistros: index === 0 ? 500 : 0
          })
        })
      }
    })

    // Empresas na fila
    status.queue.forEach((item) => {
      if (!status.contractsInProcessing.includes(item.idEmpresa)) {
        TABELAS.forEach((tabela) => {
          const key = `${item.idEmpresa}-${tabela}`
          newProgress.set(key, {
            empresa: item.empresa,
            tabela,
            status: 'aguardando',
            fase: 'Na fila de sincronização'
          })
        })
      }
    })

    setSyncProgress(newProgress)
  }

  const formatarData = (dataStr: string) => {
    const data = new Date(dataStr)
    return data.toLocaleString('pt-BR')
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'processando':
        return 'bg-blue-100 text-blue-800'
      case 'concluido':
        return 'bg-green-100 text-green-800'
      case 'erro':
        return 'bg-red-100 text-red-800'
      default:
        return 'bg-gray-100 text-gray-800'
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'processando':
        return <RefreshCw className="w-3 h-3 animate-spin" />
      case 'concluido':
        return <CheckCircle className="w-3 h-3" />
      case 'aguardando':
        return <Clock className="w-3 h-3" />
      default:
        return <Database className="w-3 h-3" />
    }
  }

  const calcularProgresso = (progress: SyncProgress) => {
    if (progress.totalPaginas && progress.paginaAtual) {
      return (progress.paginaAtual / progress.totalPaginas) * 100
    }
    if (progress.totalRegistros && progress.registrosProcessados) {
      return (progress.registrosProcessados / progress.totalRegistros) * 100
    }
    return 0
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
            <h1 className="text-3xl font-bold">Monitor de Sincronização</h1>
            <p className="text-muted-foreground mt-2">
              Acompanhe em tempo real o progresso das sincronizações
            </p>
          </div>
          <div className="flex gap-2 items-center">
            <Badge variant={autoRefresh ? "default" : "outline"} className="gap-2">
              {autoRefresh ? <Play className="w-3 h-3" /> : <Pause className="w-3 h-3" />}
              Auto-refresh {autoRefresh ? 'Ativo' : 'Pausado'}
            </Badge>
            <button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className="text-sm text-muted-foreground hover:text-foreground"
            >
              {autoRefresh ? 'Pausar' : 'Retomar'}
            </button>
          </div>
        </div>

        {/* Status Geral */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Status do Sistema</CardDescription>
              <CardTitle className="text-2xl flex items-center gap-2">
                {queueStatus?.isSyncRunning ? (
                  <>
                    <RefreshCw className="w-5 h-5 animate-spin text-blue-600" />
                    <span className="text-blue-600">Sincronizando</span>
                  </>
                ) : (
                  <>
                    <CheckCircle className="w-5 h-5 text-green-600" />
                    <span className="text-green-600">Ocioso</span>
                  </>
                )}
              </CardTitle>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Empresas na Fila</CardDescription>
              <CardTitle className="text-3xl">{queueStatus?.queueLength || 0}</CardTitle>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Em Processamento</CardDescription>
              <CardTitle className="text-3xl text-blue-600">
                {queueStatus?.contractsInProcessing.length || 0}
              </CardTitle>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardDescription>Tabelas por Empresa</CardDescription>
              <CardTitle className="text-3xl">{TABELAS.length}</CardTitle>
            </CardHeader>
          </Card>
        </div>

        {/* Fila de Sincronização */}
        {queueStatus && queueStatus.queue.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Fila de Sincronização</CardTitle>
              <CardDescription>
                Ordem de processamento das empresas
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {queueStatus.queue.map((item, index) => {
                  const isProcessing = queueStatus.contractsInProcessing.includes(item.idEmpresa)
                  return (
                    <div
                      key={`${item.idEmpresa}-${index}`}
                      className={`p-4 rounded-lg border ${
                        isProcessing ? 'border-blue-500 bg-blue-50' : 'border-gray-200'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <Badge variant="outline" className="font-mono">
                            #{index + 1}
                          </Badge>
                          {isProcessing && (
                            <RefreshCw className="w-4 h-4 animate-spin text-blue-600" />
                          )}
                          <div>
                            <p className="font-semibold">{item.empresa}</p>
                            <p className="text-xs text-muted-foreground">
                              ID: {item.idEmpresa}
                            </p>
                          </div>
                        </div>
                        <div className="text-right">
                          <Badge className={isProcessing ? 'bg-blue-600' : ''}>
                            {isProcessing ? 'Processando' : 'Aguardando'}
                          </Badge>
                          <p className="text-xs text-muted-foreground mt-1">
                            {formatarData(item.timestamp)}
                          </p>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Progresso Detalhado por Empresa */}
        {queueStatus?.contractsInProcessing && queueStatus.contractsInProcessing.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Progresso em Tempo Real</CardTitle>
              <CardDescription>
                Detalhamento do processamento atual
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                {queueStatus.contractsInProcessing.map((idEmpresa) => {
                  const empresaProgress = Array.from(syncProgress.entries()).filter(
                    ([key]) => key.startsWith(`${idEmpresa}-`)
                  )

                  if (empresaProgress.length === 0) return null

                  const empresa = empresaProgress[0][1].empresa

                  return (
                    <div key={idEmpresa} className="border rounded-lg p-4 space-y-4">
                      <div className="flex items-center justify-between">
                        <h3 className="text-lg font-semibold flex items-center gap-2">
                          <RefreshCw className="w-5 h-5 animate-spin text-blue-600" />
                          {empresa}
                        </h3>
                        <Badge variant="outline">ID: {idEmpresa}</Badge>
                      </div>

                      <div className="space-y-3">
                        {empresaProgress.map(([key, progress]) => (
                          <div key={key} className="space-y-2">
                            <div className="flex items-center justify-between">
                              <div className="flex items-center gap-2">
                                <Badge className={getStatusColor(progress.status)}>
                                  {getStatusIcon(progress.status)}
                                  <span className="ml-1">{progress.tabela}</span>
                                </Badge>
                                {progress.fase && (
                                  <span className="text-sm text-muted-foreground">
                                    {progress.fase}
                                  </span>
                                )}
                              </div>
                              {progress.paginaAtual && progress.totalPaginas && (
                                <span className="text-sm font-mono text-muted-foreground">
                                  Página {progress.paginaAtual}/{progress.totalPaginas}
                                </span>
                              )}
                            </div>

                            {progress.status === 'processando' && (
                              <div className="space-y-1">
                                <Progress value={calcularProgresso(progress)} className="h-2" />
                                {progress.registrosProcessados !== undefined && (
                                  <div className="flex justify-between text-xs text-muted-foreground">
                                    <span>{progress.registrosProcessados} registros processados</span>
                                    {progress.totalRegistros && (
                                      <span>Total: {progress.totalRegistros}</span>
                                    )}
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )
                })}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Mensagem quando não há sincronizações */}
        {(!queueStatus || queueStatus.queueLength === 0) && !queueStatus?.isSyncRunning && (
          <Card>
            <CardContent className="text-center py-12">
              <CheckCircle className="w-16 h-16 mx-auto mb-4 text-green-600 opacity-50" />
              <p className="text-lg font-medium">Nenhuma sincronização em andamento</p>
              <p className="text-sm text-muted-foreground mt-2">
                O sistema está aguardando novas sincronizações
              </p>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  )
}
