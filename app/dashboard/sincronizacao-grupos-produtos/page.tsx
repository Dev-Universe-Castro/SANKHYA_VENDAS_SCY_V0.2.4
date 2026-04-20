"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authService } from "@/lib/auth-service"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/components/ui/use-toast"
import { RefreshCw, Clock, Database, Tag, Package } from "lucide-react"
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
    totalRegistros: number;
    registrosInseridos: number;
    registrosAtualizados: number;
    registrosDeletados: number
    dataInicio: string
    dataFim: string
    duracao: number
    erro?: string
}

export default function SincronizacaoGruposProdutosPage() {
    const router = useRouter()
    const { toast } = useToast()
    const [contratos, setContratos] = useState<Contrato[]>([])
    const [estatisticas, setEstatisticas] = useState<Map<number, EstatisticaSync>>(new Map())
    const [grupos, setGrupos] = useState<any[]>([])
    const [loading, setLoading] = useState(true)
    const [syncingAll, setSyncingAll] = useState(false)
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

            const statsRes = await fetch('/api/sync/grupos-produtos')
            if (statsRes.ok) {
                const statsData = await statsRes.json()
                const statsMap = new Map<number, EstatisticaSync>()
                statsData.forEach((stat: EstatisticaSync) => {
                    statsMap.set(stat.ID_SISTEMA, stat)
                })
                setEstatisticas(statsMap)
            }

            fetch('/api/sync/grupos-produtos?list=true')
                .then(res => res.json())
                .then(data => setGrupos(data))
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

    const sincronizarTodas = async () => {
        try {
            setSyncingAll(true)
            toast({
                title: "Sincronização iniciada",
                description: "Sincronizando grupos de de todas as empresas..."
            })

            const response = await fetch('/api/sync/grupos-produtos', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ syncAll: true })
            })

            if (!response.ok) {
                throw new Error('Erro ao sincronizar')
            }

            toast({
                title: "Processo iniciado",
                description: "A sincronização continuará em segundo plano.",
            })

            setTimeout(() => loadData(), 2000)

        } catch (error) {
            console.error('Erro ao sincronizar:', error)
            toast({
                title: "Erro",
                description: "Erro ao iniciar sincronização",
                variant: "destructive"
            })
        } finally {
            setSyncingAll(false)
        }
    }

    const sincronizarEmpresa = async (idSistema: number, empresa: string, type: 'total' | 'partial' = 'total') => {
        try {
            setSyncingOne(idSistema)
            toast({
                title: `Sincronização ${type === 'partial' ? 'Parcial' : 'Total'} iniciada`,
                description: `Sincronizando grupos de ${empresa}...`
            })

            const response = await fetch('/api/sync/grupos-produtos', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ idSistema, empresa, type })
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
                <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                    <div>
                        <h1 className="text-3xl font-bold">Sincronização de Grupos de Produtos</h1>
                        <p className="text-muted-foreground mt-2">
                            Gerencie a sincronização de Grupos de Produtos entre Sankhya e o Oracle.
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

                <div className="grid gap-6 md:grid-cols-3">
                    <Card>
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">Total de Grupos</CardTitle>
                            <Package className="h-4 w-4 text-muted-foreground" />
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">
                                {Array.from(estatisticas.values()).reduce((acc, curr) => acc + curr.REGISTROS_ATIVOS, 0)}
                            </div>
                            <p className="text-xs text-muted-foreground">
                                Grupos ativos sincronizados
                            </p>
                        </CardContent>
                    </Card>
                </div>

                <Card>
                    <CardHeader>
                        <CardTitle>Empresas Cadastradas (Contratos)</CardTitle>
                        <CardDescription>
                            Status de sincronização dos grupos por contrato.
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        {contratos.length === 0 ? (
                            <div className="text-center py-12">
                                <Tag className="w-16 h-16 mx-auto mb-4 text-muted-foreground opacity-50" />
                                <p className="text-lg font-medium">Nenhuma empresa ativa encontrada</p>
                            </div>
                        ) : (
                            <div className="overflow-x-auto">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead>ID</TableHead>
                                            <TableHead>Empresa (Contrato)</TableHead>
                                            <TableHead>CNPJ</TableHead>
                                            <TableHead className="text-center">Total Grupos Sync</TableHead>
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
                                                                {stats ? formatarData(stats.ULTIMA_SINCRONIZACAO) : 'Nunca'}
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

                <Card>
                    <CardHeader>
                        <CardTitle>Grupos de Produtos Sincronizados (Detalhes)</CardTitle>
                        <CardDescription>
                            Lista de grupos sincronizados do Sankhya.
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="rounded-md border overflow-x-auto">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>Cód. Grupo</TableHead>
                                        <TableHead>Descrição</TableHead>
                                        <TableHead>Contrato Origem</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Atualizado em</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {grupos.length === 0 ? (
                                        <TableRow>
                                            <TableCell colSpan={5} className="text-center py-8 text-muted-foreground">
                                                Nenhum grupo sincronizado encontrado.
                                            </TableCell>
                                        </TableRow>
                                    ) : (
                                        grupos.map((grupo) => (
                                            <TableRow key={`${grupo.ID_SISTEMA}-${grupo.CODGRUPOPROD}`}>
                                                <TableCell>{grupo.CODGRUPOPROD}</TableCell>
                                                <TableCell>{grupo.DESCRGRUPOPROD}</TableCell>
                                                <TableCell className="text-xs text-muted-foreground">
                                                    {grupo.NOME_CONTRATO} (ID: {grupo.ID_SISTEMA})
                                                </TableCell>
                                                <TableCell>
                                                    {grupo.SANKHYA_ATUAL === 'S' ? (
                                                        <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">Ativo</Badge>
                                                    ) : (
                                                        <Badge variant="outline" className="bg-red-50 text-red-700 border-red-200">Inativo</Badge>
                                                    )}
                                                </TableCell>
                                                <TableCell className="text-xs text-muted-foreground">
                                                    {formatarData(grupo.DT_ULT_CARGA)}
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
