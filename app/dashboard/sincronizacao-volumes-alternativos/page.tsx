"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/components/ui/use-toast"
import { RefreshCw, Clock, Database, Box } from "lucide-react"
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
  empresa: string;
  totalRegistros: number
  registrosInseridos: number
  registrosAtualizados: number
  registrosDeletados: number
  dataInicio: string
  dataFim: string
  duracao: number
  erro?: string
}

export default function SincronizacaoVolumesAlternativosPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [estatisticas, setEstatisticas] = useState<Map<number, EstatisticaSync>>(new Map())
  const [volumes, setVolumes] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [syncingOne, setSyncingOne] = useState<number | null>(null)

  useEffect(() => {
    const checkAuth = () => {
      const user = authService.getCurrentUser()
      if (!user) {
        router.push("/login")
      }
    }
    checkAuth()
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

      const statsRes = await fetch('/api/sync/volumes-alternativos')
      if (statsRes.ok) {
        const statsData = await statsRes.json()
        const statsMap = new Map<number, EstatisticaSync>()
        statsData.forEach((stat: EstatisticaSync) => {
          statsMap.set(stat.ID_SISTEMA, stat)
        })
        setEstatisticas(statsMap)
      }

      fetch('/api/sync/volumes-alternativos?list=true')
        .then(res => res.json())
        .then(data => {
          if (Array.isArray(data)) {
            setVolumes(data);
          } else {
            console.error('Dados de volumes inválidos:', data);
            setVolumes([]);
          }
        })
        .catch(console.error)

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

  const sincronizarEmpresa = async (idSistema: number, empresa: string) => {
    try {
      setSyncingOne(idSistema)
      toast({
        title: `Sincronização iniciada`,
        description: `Sincronizando volumes alternativos de ${empresa}...`
      })

      const response = await fetch('/api/sync/volumes-alternativos', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ idSistema, empresa, type: 'total' }),
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
        <div>
          <h1 className="text-3xl font-bold">Volumes Alternativos</h1>
          <p className="text-muted-foreground mt-2">
            Sincronização de unidades e volumes alternativos dos produtos do Sankhya.
          </p>
        </div>

        <div className="grid gap-6 md:grid-cols-3">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total de Volumes</CardTitle>
              <Box className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {Array.from(estatisticas.values()).reduce((acc, curr) => acc + curr.REGISTROS_ATIVOS, 0)}
              </div>
              <p className="text-xs text-muted-foreground">
                Unidades alternativas sincronizadas
              </p>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Contratos Ativos</CardTitle>
            <CardDescription>
              Status de sincronização de volumes por empresa.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {contratos.length === 0 ? (
              <p>Nenhuma empresa ativa encontrada.</p>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>ID</TableHead>
                      <TableHead>Empresa</TableHead>
                      <TableHead className="text-center">Registros</TableHead>
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
                          <TableCell><Badge variant="outline">{contrato.ID_EMPRESA}</Badge></TableCell>
                          <TableCell className="font-medium">{contrato.EMPRESA}</TableCell>
                          <TableCell className="text-center">
                            {stats ? (
                              <Badge className="bg-green-100 text-green-800 hover:bg-green-200">
                                {stats.REGISTROS_ATIVOS}
                              </Badge>
                            ) : '-'}
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {stats ? formatarData(stats.ULTIMA_SINCRONIZACAO) : 'Nunca'}
                          </TableCell>
                          <TableCell className="text-right">
                            <Button
                              onClick={() => sincronizarEmpresa(contrato.ID_EMPRESA, contrato.EMPRESA)}
                              disabled={isSyncing}
                              size="sm"
                              className="gap-2"
                            >
                              {isSyncing ? (
                                <RefreshCw className="w-4 h-4 animate-spin" />
                              ) : (
                                <RefreshCw className="w-4 h-4" />
                              )}
                              Sincronizar
                            </Button>
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

        <Card>
          <CardHeader>
            <CardTitle>Detalhes dos Volumes Sincronizados</CardTitle>
            <CardDescription>
              Amostra dos últimos 500 registros sincronizados.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="rounded-md border overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Cód. Prod.</TableHead>
                    <TableHead>Unidade (Vol.)</TableHead>
                    <TableHead>Fator (Qtd)</TableHead>
                    <TableHead>Empresa</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {volumes.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center py-8 text-muted-foreground">
                        Nenhum registro encontrado.
                      </TableCell>
                    </TableRow>
                  ) : (
                    volumes.map((vol, index) => (
                      <TableRow key={index}>
                        <TableCell className="font-medium">{vol.CODPROD}</TableCell>
                        <TableCell><Badge variant="secondary">{vol.CODVOL}</Badge></TableCell>
                        <TableCell>{vol.QUANTIDADE}</TableCell>
                        <TableCell className="text-xs">{vol.NOME_CONTRATO}</TableCell>
                        <TableCell>
                          <Badge variant={vol.SANKHYA_ATUAL === 'S' ? 'default' : 'destructive'} className="text-[10px]">
                            {vol.SANKHYA_ATUAL === 'S' ? 'ATIVO' : 'INATIVO'}
                          </Badge>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  )
}
