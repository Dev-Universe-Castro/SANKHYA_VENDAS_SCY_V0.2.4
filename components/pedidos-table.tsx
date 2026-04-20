'use client'

import { useState, useEffect, useCallback } from 'react'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Search, FileText } from 'lucide-react'
import { toast } from 'sonner'
import { authService } from '@/lib/auth-service'

interface Pedido {
  NUNOTA: string
  CODPARC: string
  NOMEPARC: string
  CODVEND: string
  NOMEVEND: string
  VLRNOTA: number
  DTNEG: string
}

// Define the Partner interface for clarity
interface Partner {
  CODPARC: string;
  NOMEPARC: string;
  RAZAOSOCIAL?: string; // Optional as it might not always be present
  CGC_CPF?: string;    // Optional
}

export default function PedidosTable() {
  const [pedidos, setPedidos] = useState<Pedido[]>([])
  const [loading, setLoading] = useState(false)
  const [numeroPedido, setNumeroPedido] = useState('')
  const [nomeCliente, setNomeCliente] = useState('')
  const [dataInicio, setDataInicio] = useState('')
  const [dataFim, setDataFim] = useState('')
  const [currentPage, setCurrentPage] = useState(1)
  const [itemsPerPage] = useState(20)

  // State for partner search
  const [parceiroSelecionado, setParceiroSelecionado] = useState<string>("")
  const [dataNegociacaoInicio, setDataNegociacaoInicio] = useState<string>("")
  const [dataNegociacaoFinal, setDataNegociacaoFinal] = useState<string>("")
  const [tipoFinanceiro, setTipoFinanceiro] = useState<string>("3") // 1=Pendente, 2=Baixado, 3=Todos
  const [statusFinanceiro, setStatusFinanceiro] = useState<string>("3") // 1=Real, 2=Provisão, 3=Todos

  // Busca de parceiros
  const [parceiros, setParceiros] = useState<Partner[]>([])
  const [partnerSearch, setPartnerSearch] = useState("")
  const [isLoadingPartners, setIsLoadingPartners] = useState(false)
  const [allParceiros, setAllParceiros] = useState<Partner[]>([]) // To store all partners from cache
  const [showPartnerDropdown, setShowPartnerDropdown] = useState(false)

  useEffect(() => {
    // Não carregar pedidos automaticamente
    setPedidos([])
  }, [])


  // Carrega parceiros do cache
  useEffect(() => {
    const loadPartners = async () => {
      try {
        const cachedParceiros = sessionStorage.getItem('cached_parceiros')
        if (cachedParceiros) {
          try {
            const parsedCache = JSON.parse(cachedParceiros)
            const loadedParceiros = parsedCache.parceiros || parsedCache
            setAllParceiros(Array.isArray(loadedParceiros) ? loadedParceiros : [])
            setParceiros([]) // Começa vazio até digitar 2 caracteres
            console.log('✅ Parceiros carregados do cache (Pedidos):', loadedParceiros.length)
            return
          } catch (e) {
            console.error('Erro ao parsear cache:', e)
            sessionStorage.removeItem('cached_parceiros')
          }
        }

        console.warn('⚠️ Cache de parceiros vazio')
        setAllParceiros([])
        setParceiros([])
      } catch (error) {
        console.error('Erro ao carregar parceiros:', error)
        setAllParceiros([])
        setParceiros([])
      }
    }
    loadPartners();
  }, [])

  const handlePartnerSearch = (value: string) => {
    setPartnerSearch(value)
    setNomeCliente(value)

    // Só buscar se tiver 2+ caracteres
    if (value.length < 2) {
      setParceiros([])
      setShowPartnerDropdown(false)
      return
    }

    try {
      const cachedParceiros = sessionStorage.getItem('cached_parceiros')
      if (cachedParceiros) {
        const parsedCache = JSON.parse(cachedParceiros)
        const allParceirosData = parsedCache.parceiros || parsedCache
        const searchLower = value.toLowerCase()
        const filtered = allParceirosData.filter((p: any) =>
          p.NOMEPARC?.toLowerCase().includes(searchLower) ||
          p.CGC_CPF?.includes(value) ||
          p.RAZAOSOCIAL?.toLowerCase().includes(searchLower) ||
          p.CODPARC?.toString().includes(value)
        )
        setParceiros(filtered)
        setShowPartnerDropdown(true)
        console.log('✅ Parceiros filtrados (PedidosTable):', filtered.length)
      } else {
        setParceiros([])
        setShowPartnerDropdown(false)
      }
    } catch (error) {
      console.error('Erro ao filtrar parceiros:', error)
      setParceiros([])
      setShowPartnerDropdown(false)
    }
  }

  const handlePartnerSelect = (partner: Partner) => {
    setNomeCliente(partner.CODPARC) // Usar CODPARC para filtrar pedidos
    setPartnerSearch(partner.NOMEPARC) // Mostrar NOMEPARC no campo de busca
    setShowPartnerDropdown(false)
  }

  const carregarPedidos = async (filtroDataInicio?: string, filtroDataFim?: string, filtroNumeroPedido?: string, filtroNomeCliente?: string) => {
    // Validar se pelo menos um filtro foi aplicado
    if (!filtroDataInicio && !filtroDataFim && !filtroNumeroPedido && !filtroNomeCliente) {
      toast.error("Por favor, aplique pelo menos um filtro antes de buscar os pedidos");
      return;
    }

    try {
      setLoading(true);
      const user = authService.getCurrentUser();

      if (!user) {
        toast.error("Usuário não autenticado. Faça login novamente.");
        return;
      }

      const params = new URLSearchParams({
        userId: user.id.toString(),
        ...(filtroDataInicio && { dataInicio: filtroDataInicio }),
        ...(filtroDataFim && { dataFim: filtroDataFim }),
        ...(filtroNumeroPedido && { numeroPedido: filtroNumeroPedido }),
        ...(filtroNomeCliente && { nomeCliente: filtroNomeCliente })
      });

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 15000)

      const response = await fetch(`/api/sankhya/pedidos/listar?${params.toString()}`, {
        signal: controller.signal,
        headers: {
          'Cache-Control': 'public, max-age=60',
          'Accept': 'application/json'
        }
      });

      clearTimeout(timeoutId)

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || 'Erro ao buscar pedidos');
      }

      const data = await response.json();
      setPedidos(Array.isArray(data) ? data : []);
      toast.success(`${Array.isArray(data) ? data.length : 0} pedido(s) encontrado(s)`);
    } catch (error: any) {
      console.error('Erro ao buscar pedidos:', error);
      toast.error(error.name === 'AbortError'
        ? "Tempo de carregamento excedido"
        : error.message || "Erro ao carregar pedidos"
      );
      setPedidos([]);
    } finally {
      setLoading(false);
    }
  };

  const formatarData = (data: string) => {
    if (!data) return 'N/A'
    // Se vier no formato DD/MM/YYYY, retorna como está
    if (data.includes('/')) return data
    // Se vier no formato YYYY-MM-DD, converte
    const [ano, mes, dia] = data.split('-')
    return `${dia}/${mes}/${ano}`
  }

  const formatarValor = (valor: number) => {
    return new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL'
    }).format(valor)
  }

  // Lógica de paginação
  const indexOfLastItem = currentPage * itemsPerPage
  const indexOfFirstItem = indexOfLastItem - itemsPerPage
  const currentPedidos = pedidos.slice(indexOfFirstItem, indexOfLastItem)
  const totalPages = Math.ceil(pedidos.length / itemsPerPage)

  const handlePageChange = (pageNumber: number) => {
    setCurrentPage(pageNumber)
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  // Resetar página ao carregar novos pedidos
  useEffect(() => {
    setCurrentPage(1)
  }, [pedidos])

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <div>
            <CardTitle className="text-2xl font-bold text-foreground">Pedidos de Venda</CardTitle>
            <CardDescription className="text-sm text-muted-foreground">
              Gerencie os pedidos de venda no sistema Sankhya
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent>
          {/* Filtros de Busca */}
          <div className="bg-white border rounded-lg p-3 md:p-4 space-y-3 md:space-y-4 mb-4">
            <div className="flex items-center justify-between mb-2 md:mb-3">
              <h3 className="text-sm font-semibold text-gray-700">Filtros de Busca</h3>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
              {/* Número do Pedido */}
              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="numeroPedido" className="text-xs md:text-sm font-medium">
                  Número do Pedido
                </Label>
                <Input
                  id="numeroPedido"
                  type="number"
                  placeholder="Ex: 123456"
                  value={numeroPedido}
                  onChange={(e) => setNumeroPedido(e.target.value)}
                  className="h-9 md:h-10 text-sm"
                />
              </div>

              {/* Nome do Cliente */}
              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="nomeCliente" className="text-xs md:text-sm font-medium">
                  Nome do Cliente
                </Label>
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4 z-10" />
                  <Input
                    id="nomeCliente"
                    placeholder="Digite para buscar cliente..."
                    value={partnerSearch}
                    onChange={(e) => handlePartnerSearch(e.target.value)}
                    onFocus={() => {
                      if (partnerSearch.length >= 2) {
                        setShowPartnerDropdown(true)
                      }
                    }}
                    className="pl-10 h-9 md:h-10 text-sm"
                  />
                  {showPartnerDropdown && partnerSearch.length >= 2 && (
                    <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-60 overflow-auto">
                      {isLoadingPartners ? (
                        <div className="p-3 text-sm text-center text-muted-foreground">Carregando...</div>
                      ) : parceiros.length === 0 ? (
                        <div className="p-3 text-sm text-center text-muted-foreground">
                          Nenhum cliente encontrado
                        </div>
                      ) : (
                        parceiros.map((partner) => (
                          <div
                            key={partner.CODPARC}
                            onClick={() => handlePartnerSelect(partner)}
                            className="p-3 hover:bg-gray-100 cursor-pointer border-b last:border-b-0"
                          >
                            <div className="font-medium text-sm">{partner.NOMEPARC}</div>
                            <div className="text-xs text-muted-foreground">
                              Cód: {partner.CODPARC} {partner.CGC_CPF && `• ${partner.CGC_CPF}`}
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  )}
                </div>
              </div>

              {/* Data de Negociação Início */}
              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="dataInicio" className="text-xs md:text-sm font-medium">
                  Data Negociação (Início)
                </Label>
                <Input
                  id="dataInicio"
                  type="date"
                  value={dataInicio}
                  onChange={(e) => setDataInicio(e.target.value)}
                  className="h-9 md:h-10 text-sm"
                />
              </div>

              {/* Data de Negociação Fim */}
              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="dataFim" className="text-xs md:text-sm font-medium">
                  Data Negociação (Fim)
                </Label>
                <Input
                  id="dataFim"
                  type="date"
                  value={dataFim}
                  onChange={(e) => setDataFim(e.target.value)}
                  className="h-9 md:h-10 text-sm"
                />
              </div>

              {/* Botão de Buscar */}
              <div className="space-y-1.5 md:space-y-2">
                <Label className="text-xs md:text-sm font-medium opacity-0 hidden md:block">Ação</Label>
                <Button 
                  onClick={() => carregarPedidos(dataInicio, dataFim, numeroPedido, nomeCliente)}
                  disabled={loading}
                  className="w-full h-9 md:h-10 text-sm bg-green-600 hover:bg-green-700"
                >
                  <Search className="w-4 h-4 mr-2" />
                  {loading ? 'Buscando...' : 'Buscar Pedidos'}
                </Button>
              </div>
            </div>
          </div>



          {/* Informações sobre os resultados */}
          {pedidos.length > 0 && (
            <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-4">
              <div className="flex items-center gap-2 text-sm text-green-800">
                <span className="font-semibold">Exibindo:</span>
                <Badge variant="outline" className="bg-white">
                  {indexOfFirstItem + 1} - {Math.min(indexOfLastItem, pedidos.length)} de {pedidos.length} pedido(s)
                </Badge>
                <span className="text-xs text-green-600">
                  (Página {currentPage} de {totalPages})
                </span>
              </div>
            </div>
          )}

          {/* Tabela */}
          <div className="border rounded-lg overflow-hidden">
            <div className="overflow-y-auto max-h-[600px]">
              <Table>
                <TableHeader className="sticky top-0 z-10" style={{ backgroundColor: 'rgb(35, 55, 79)' }}>
                  <TableRow>
                    <TableHead className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">NUNOTA</TableHead>
                    <TableHead className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">Parceiro</TableHead>
                    <TableHead className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">Vendedor</TableHead>
                    <TableHead className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">Data Negociação</TableHead>
                    <TableHead className="px-6 py-4 text-right text-sm font-semibold text-white uppercase tracking-wider">Valor</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loading ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center py-12">
                        <div className="flex flex-col items-center gap-3">
                          <div className="inline-block animate-spin rounded-full h-10 w-10 border-4 border-green-500 border-t-transparent"></div>
                          <p className="text-sm font-medium text-gray-700">Buscando pedidos...</p>
                          <p className="text-xs text-gray-500">Isso pode levar alguns segundos</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : pedidos.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center py-8 text-gray-500">
                        {numeroPedido || nomeCliente || dataInicio || dataFim 
                          ? 'Nenhum pedido encontrado com os critérios de busca' 
                          : 'Utilize os filtros acima para buscar pedidos'}
                      </TableCell>
                    </TableRow>
                  ) : (
                    currentPedidos.map((pedido) => (
                      <TableRow key={pedido.NUNOTA} className="hover:bg-gray-50">
                        <TableCell className="font-medium">
                          <Badge variant="outline" className="border-green-300 text-green-700">
                            {pedido.NUNOTA}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div>
                            <div className="font-medium text-gray-900">{pedido.NOMEPARC}</div>
                            <div className="text-xs text-gray-500">Cód: {pedido.CODPARC}</div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div>
                            <div className="font-medium text-gray-900">{pedido.NOMEVEND}</div>
                            <div className="text-xs text-gray-500">Cód: {pedido.CODVEND}</div>
                          </div>
                        </TableCell>
                        <TableCell className="text-gray-700">
                          {formatarData(pedido.DTNEG)}
                        </TableCell>
                        <TableCell className="text-right font-semibold text-green-700">
                          {formatarValor(pedido.VLRNOTA)}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </div>

          {/* Controles de Paginação */}
          {pedidos.length > 0 && totalPages > 1 && (
            <div className="flex items-center justify-between mt-4 px-2">
              <div className="text-sm text-gray-600">
                Mostrando {indexOfFirstItem + 1} a {Math.min(indexOfLastItem, pedidos.length)} de {pedidos.length} resultados
              </div>

              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                  className="h-8"
                >
                  Anterior
                </Button>

                <div className="flex gap-1">
                  {[...Array(totalPages)].map((_, index) => {
                    const pageNumber = index + 1

                    // Mostrar apenas páginas próximas à atual
                    if (
                      pageNumber === 1 ||
                      pageNumber === totalPages ||
                      (pageNumber >= currentPage - 2 && pageNumber <= currentPage + 2)
                    ) {
                      return (
                        <Button
                          key={pageNumber}
                          variant={currentPage === pageNumber ? "default" : "outline"}
                          size="sm"
                          onClick={() => handlePageChange(pageNumber)}
                          className={`h-8 w-8 p-0 ${
                            currentPage === pageNumber 
                              ? 'bg-green-600 hover:bg-green-700' 
                              : ''
                          }`}
                        >
                          {pageNumber}
                        </Button>
                      )
                    } else if (
                      pageNumber === currentPage - 3 ||
                      pageNumber === currentPage + 3
                    ) {
                      return <span key={pageNumber} className="px-1">...</span>
                    }
                    return null
                  })}
                </div>

                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                  className="h-8"
                >
                  Próxima
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}