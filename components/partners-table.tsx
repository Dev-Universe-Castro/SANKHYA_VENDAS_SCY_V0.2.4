"use client"

import { useState, useEffect } from "react"
import { Search, Pencil, ChevronLeft, ChevronRight, Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { PartnerModal } from "@/components/partner-modal"
import { useToast } from "@/hooks/use-toast"
import { authService } from "@/lib/auth-service"

interface Partner {
  _id: string
  CODPARC: string
  NOMEPARC: string
  CGC_CPF: string
  CODCID?: string
  ATIVO?: string
  TIPPESSOA?: string
  CODVEND?: number
  CLIENTE?: string
}

interface PaginatedResponse {
  parceiros: Partner[]
  total: number
  page: number
  pageSize: number
  totalPages: number
}

const ITEMS_PER_PAGE = 50

export default function PartnersTable() {
  const [searchName, setSearchName] = useState("")
  const [searchCode, setSearchCode] = useState("")
  const [appliedSearchName, setAppliedSearchName] = useState("")
  const [appliedSearchCode, setAppliedSearchCode] = useState("")
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [selectedPartner, setSelectedPartner] = useState<Partner | null>(null)
  const [partners, setPartners] = useState<Partner[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [totalRecords, setTotalRecords] = useState(0)
  const { toast } = useToast()
  const [currentUser, setCurrentUser] = useState<any>(null)
  const [vendedoresMap, setVendedoresMap] = useState<Record<number, string>>({})
  const [searchTerm, setSearchTerm] = useState("") // Estado para o termo de busca geral
  const [showPartnerDropdown, setShowPartnerDropdown] = useState(false)
  const [filteredPartners, setFilteredPartners] = useState<Partner[]>([])

  useEffect(() => {
    loadPartners(appliedSearchName, appliedSearchCode)
  }, [currentPage, appliedSearchName, appliedSearchCode])

  useEffect(() => {
    const user = authService.getCurrentUser()
    if (user) {
      setCurrentUser(user)
    }
    loadVendedores()
  }, [])

  const loadVendedores = async () => {
    try {
      const response = await fetch('/api/vendedores?tipo=todos')
      const vendedores = await response.json()
      const map: Record<number, string> = {}
      vendedores.forEach((v: any) => {
        map[v.CODVEND] = v.APELIDO
      })
      setVendedoresMap(map)
    } catch (error) {
      console.error('Erro ao carregar vendedores:', error)
    }
  }

  const handleSearch = () => {
    setAppliedSearchName(searchName)
    setAppliedSearchCode(searchCode)
    setCurrentPage(1)
  }

  const handleSearchKeyPress = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSearch()
    }
  }

  const handlePreviousPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1)
    }
  }

  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1)
    }
  }

  const startIndex = (currentPage - 1) * ITEMS_PER_PAGE
  const endIndex = Math.min(startIndex + ITEMS_PER_PAGE, totalRecords)

  const loadPartners = async (searchQueryName = '', searchQueryCode = '') => {
    setIsLoading(true)
    try {
      // Tentar carregar do cache primeiro
      const cachedParceiros = sessionStorage.getItem('cached_parceiros');
      if (cachedParceiros) {
        try {
          const parsedData = JSON.parse(cachedParceiros);
          // O cache pode vir como array direto ou como objeto com propriedade 'parceiros'
          const parceiros = Array.isArray(parsedData) ? parsedData : (parsedData.parceiros || []);

          if (parceiros.length > 0) {
            console.log(`📦 Cache encontrado com ${parceiros.length} parceiros`);

            // Aplicar filtros localmente
            let filtered = parceiros;
            if (appliedSearchName) {
              filtered = filtered.filter((p: any) => 
                p.NOMEPARC?.toLowerCase().includes(appliedSearchName.toLowerCase())
              );
            }
            if (appliedSearchCode) {
              filtered = filtered.filter((p: any) => 
                p.CODPARC?.toString().includes(appliedSearchCode)
              );
            }

            // Paginar
            const start = (currentPage - 1) * ITEMS_PER_PAGE;
            const paginatedParceiros = filtered.slice(start, start + ITEMS_PER_PAGE);

            const data: PaginatedResponse = {
              parceiros: paginatedParceiros,
              total: filtered.length,
              page: currentPage,
              pageSize: ITEMS_PER_PAGE,
              totalPages: Math.ceil(filtered.length / ITEMS_PER_PAGE)
            };

            console.log(`✅ Exibindo ${data.parceiros.length} parceiros do cache (página ${currentPage}/${data.totalPages})`);
            setPartners(data.parceiros);
            setTotalPages(data.totalPages);
            setTotalRecords(data.total);
            setIsLoading(false);
            return;
          } else {
            console.warn('⚠️ Cache vazio, removendo...');
            sessionStorage.removeItem('cached_parceiros');
          }
        } catch (e) {
          console.warn('⚠️ Erro ao processar cache, removendo:', e);
          sessionStorage.removeItem('cached_parceiros');
        }
      } else {
        console.log('ℹ️ Nenhum cache de parceiros encontrado');
      }

      const hasSearch = appliedSearchName || appliedSearchCode
      const urlParams = new URLSearchParams({
        page: currentPage.toString(),
        pageSize: ITEMS_PER_PAGE.toString(),
        ...(appliedSearchName && { searchName: appliedSearchName }),
        ...(appliedSearchCode && { searchCode: appliedSearchCode }),
        ...( !appliedSearchName && !appliedSearchCode && { offsetPage: null }),
        ...( !appliedSearchName && !appliedSearchCode && { disableRowsLimit: "true" }),
      })

      if (currentUser?.role === 'Vendedor' && currentUser.codVendedor) {
        urlParams.append('codVendedor', currentUser.codVendedor.toString())
      }

      if (currentUser?.role === 'Gerente' && currentUser.codVendedor) {
        urlParams.append('codVendedor', currentUser.codVendedor.toString())
        urlParams.append('isGerente', 'true')
      }

      const url = `/api/sankhya/parceiros?${urlParams.toString()}`

      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), 20000)

      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'Cache-Control': hasSearch ? 'public, max-age=300' : 'public, max-age=900'
        }
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        throw new Error('Falha ao carregar parceiros')
      }

      const data: PaginatedResponse = await response.json()
      setPartners(data.parceiros || [])
      setTotalPages(data.totalPages || 1)
      setTotalRecords(data.total || 0)

      if (!cachedParceiros) {
        sessionStorage.setItem('cached_parceiros', JSON.stringify(data))
      }

      if (currentPage === 1 && data.total > 0) {
        toast({
          title: "Sucesso",
          description: `${data.total} parceiros encontrados`,
        })
      }
    } catch (error) {
      console.error("Erro ao carregar parceiros:", error)
      toast({
        title: "Erro",
        description: error instanceof Error && error.name === 'AbortError'
          ? "Tempo de carregamento excedido"
          : "Falha ao carregar parceiros",
        variant: "destructive",
      })
      setPartners([])
    } finally {
      setIsLoading(false)
    }
  }

  const handleSave = async (partnerData: { CODPARC?: string; NOMEPARC: string; CGC_CPF: string; CODCID: string; ATIVO: string; TIPPESSOA: string; CODVEND?: number }) => {
    try {
      console.log("Frontend - Iniciando salvamento de parceiro:", partnerData);

      if (currentUser?.role === 'Vendedor' && !partnerData.CODVEND) {
        partnerData.CODVEND = currentUser.codVendedor;
      }

      const response = await fetch('/api/sankhya/parceiros/salvar', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(partnerData),
      })

      if (!response.ok) {
        const errorData = await response.json();
        console.error("Frontend - Erro na resposta da API:", errorData);
        throw new Error(errorData.error || 'Falha ao salvar parceiro')
      }

      const resultado = await response.json();

      console.log("Frontend - Parceiro salvo com sucesso:", resultado);

      toast({
        title: "Sucesso",
        description: partnerData.CODPARC ? "Parceiro atualizado com sucesso" : "Parceiro cadastrado com sucesso",
      })
      sessionStorage.removeItem('cached_parceiros');
      await loadPartners()
      setIsModalOpen(false)
    } catch (error: any) {
      console.error("Frontend - Erro ao salvar parceiro:", {
        message: error.message,
        dados: partnerData
      });

      toast({
        title: "Erro",
        description: error.message || "Falha ao salvar parceiro",
        variant: "destructive",
      })
    }
  }

  const handleDelete = async (codParceiro: string) => {
    if (!confirm("Tem certeza que deseja inativar este parceiro?")) {
      return
    }

    try {
      const response = await fetch('/api/sankhya/parceiros/deletar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codParceiro })
      })

      if (!response.ok) throw new Error('Erro ao inativar parceiro')

      toast({
        title: "Sucesso",
        description: "Parceiro inativado com sucesso",
      })

      sessionStorage.removeItem('cached_parceiros');
      loadPartners()
    } catch (error: any) {
      toast({
        title: "Erro",
        description: error.message || "Erro ao inativar parceiro",
        variant: "destructive",
      })
    }
  }

  const handleEdit = (partner: any) => {
    setSelectedPartner(partner)
    requestAnimationFrame(() => {
      setIsModalOpen(true)
    })
  }

  const handleCreate = () => {
    setSelectedPartner(null)
    setIsModalOpen(true)
  }

  const handlePartnerSearch = (value: string) => {
    setSearchName(value)
    setShowPartnerDropdown(true)

    // Só filtrar se tiver 2+ caracteres
    if (value.length < 2) {
      setFilteredPartners([])
      setShowPartnerDropdown(false)
      return
    }

    try {
      const cachedParceiros = sessionStorage.getItem('cached_parceiros')
      if (cachedParceiros) {
        const parsedCache = JSON.parse(cachedParceiros)
        const allParceiros = parsedCache.parceiros || parsedCache
        const searchLower = value.toLowerCase()
        const filtered = allParceiros.filter((p: any) =>
          p.NOMEPARC?.toLowerCase().includes(searchLower) ||
          p.CGC_CPF?.includes(value) ||
          p.RAZAOSOCIAL?.toLowerCase().includes(searchLower) ||
          p.CODPARC?.toString().includes(value)
        )
        setFilteredPartners(filtered)
        console.log('✅ Parceiros filtrados (PartnersTable):', filtered.length)
      }
    } catch (error) {
      console.error('Erro ao filtrar parceiros:', error)
      setFilteredPartners([])
    }
  }

  const handlePartnerSelect = (partner: Partner) => {
    setSearchName(partner.NOMEPARC)
    setShowPartnerDropdown(false)
  }


  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-2xl font-bold text-foreground">Clientes</CardTitle>
              <CardDescription className="text-sm text-muted-foreground">Consulta e gerenciamento de clientes</CardDescription>
            </div>
            <Button
              onClick={handleCreate}
              className="bg-primary hover:bg-primary/90 text-primary-foreground font-medium uppercase"
            >
              Cadastrar
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Filtros de Busca */}
          <div className="bg-white border rounded-lg p-3 md:p-4 space-y-3 md:space-y-4">
            <div className="flex items-center justify-between mb-2 md:mb-3">
              <h3 className="text-sm font-semibold text-gray-700">Filtros de Busca</h3>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 md:gap-4">
              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="searchCode" className="text-xs md:text-sm font-medium">
                  Código
                </Label>
                <Input
                  id="searchCode"
                  type="text"
                  placeholder="Buscar por código"
                  value={searchCode}
                  onChange={(e) => setSearchCode(e.target.value)}
                  onKeyPress={handleSearchKeyPress}
                  className="h-9 md:h-10 text-sm"
                />
              </div>

              <div className="space-y-1.5 md:space-y-2">
                <Label htmlFor="searchName" className="text-xs md:text-sm font-medium">
                  Nome da Empresa
                </Label>
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4 z-10" />
                  <Input
                    id="searchName"
                    type="text"
                    placeholder="Digite para buscar cliente..."
                    value={searchName}
                    onChange={(e) => handlePartnerSearch(e.target.value)}
                    onKeyPress={handleSearchKeyPress}
                    onFocus={() => {
                      if (searchName.length >= 2) {
                        setShowPartnerDropdown(true)
                      }
                    }}
                    className="pl-10 h-9 md:h-10 text-sm"
                  />
                  {showPartnerDropdown && searchName.length >= 2 && (
                    <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-60 overflow-auto">
                      {filteredPartners.length === 0 ? (
                        <div className="p-3 text-sm text-center text-muted-foreground">
                          Nenhum cliente encontrado
                        </div>
                      ) : (
                        filteredPartners.map((partner) => (
                          <div
                            key={partner.CODPARC}
                            className="p-2 hover:bg-gray-100 cursor-pointer text-sm"
                            onClick={() => handlePartnerSelect(partner)}
                          >
                            <div className="font-medium">{partner.NOMEPARC}</div>
                            <div className="text-xs text-gray-500">
                              Código: {partner.CODPARC} | CPF/CNPJ: {partner.CGC_CPF}
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  )}
                </div>
              </div>

              <div className="space-y-1.5 md:space-y-2">
                <Label className="text-xs md:text-sm font-medium opacity-0 hidden md:block">Ação</Label>
                <Button
                  onClick={handleSearch}
                  disabled={isLoading}
                  className="w-full h-9 md:h-10 text-sm bg-green-600 hover:bg-green-700"
                >
                  <Search className="w-4 h-4 mr-2" />
                  {isLoading ? 'Buscando...' : 'Buscar Clientes'}
                </Button>
              </div>
            </div>

            {(appliedSearchName || appliedSearchCode) && (
              <Button
                onClick={() => {
                  setSearchName("")
                  setSearchCode("")
                  setAppliedSearchName("")
                  setAppliedSearchCode("")
                  setCurrentPage(1)
                  sessionStorage.removeItem('cached_parceiros');
                }}
                variant="outline"
                className="w-full md:w-auto"
              >
                Limpar Filtros
              </Button>
            )}
          </div>

          {/* Tabela */}
          <div className="border rounded-lg overflow-hidden -mx-4 md:mx-0">
            <div className="overflow-x-auto overflow-y-auto max-h-[600px]">
              <table className="w-full min-w-full">
                <thead className="sticky top-0 z-10" style={{ backgroundColor: 'rgb(35, 55, 79)' }}>
                  <tr>
                    <th className="px-2 md:px-6 py-3 md:py-4 text-left text-[10px] md:text-sm font-semibold text-white uppercase tracking-wider whitespace-nowrap">
                      Código
                    </th>
                    <th className="px-2 md:px-6 py-3 md:py-4 text-left text-[10px] md:text-sm font-semibold text-white uppercase tracking-wider min-w-[150px] md:min-w-0">
                      Nome
                    </th>
                    <th className="px-2 md:px-6 py-3 md:py-4 text-left text-[10px] md:text-sm font-semibold text-white uppercase tracking-wider whitespace-nowrap">
                      CPF/CNPJ
                    </th>
                    <th className="px-2 md:px-6 py-3 md:py-4 text-left text-[10px] md:text-sm font-semibold text-white uppercase tracking-wider whitespace-nowrap hidden md:table-cell">
                      Vendedor
                    </th>
                    <th className="px-2 md:px-6 py-3 md:py-4 text-left text-[10px] md:text-sm font-semibold text-white uppercase tracking-wider whitespace-nowrap">
                      Ações
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border bg-white">
                  {isLoading ? (
                    <tr>
                      <td colSpan={5} className="px-2 md:px-6 py-4 text-center text-xs md:text-sm text-muted-foreground">
                        Carregando...
                      </td>
                    </tr>
                  ) : partners.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="px-2 md:px-6 py-4 text-center text-xs md:text-sm text-muted-foreground">
                        Nenhum cliente encontrado
                      </td>
                    </tr>
                  ) : (
                    partners.map((partner) => (
                      <tr key={partner._id} className="hover:bg-muted/50 transition-colors">
                        <td className="px-2 md:px-6 py-3 md:py-4 text-[10px] md:text-sm text-foreground whitespace-nowrap">
                          {partner.CODPARC}
                        </td>
                        <td className="px-2 md:px-6 py-3 md:py-4 text-[10px] md:text-sm text-foreground">
                          <div className="max-w-[150px] md:max-w-none truncate">
                            {partner.NOMEPARC}
                          </div>
                        </td>
                        <td className="px-2 md:px-6 py-3 md:py-4 text-[10px] md:text-sm text-foreground whitespace-nowrap">
                          {partner.CGC_CPF}
                        </td>
                        <td className="px-2 md:px-6 py-3 md:py-4 text-[10px] md:text-sm text-foreground whitespace-nowrap hidden md:table-cell">
                          {partner.CODVEND ? vendedoresMap[partner.CODVEND] || `Cód. ${partner.CODVEND}` : 'N/A'}
                        </td>
                        <td className="px-2 md:px-6 py-3 md:py-4 whitespace-nowrap">
                          <div className="flex gap-1 md:gap-2">
                            <Button
                              size="sm"
                              onClick={() => handleEdit(partner)}
                              className="bg-primary hover:bg-primary/90 text-primary-foreground font-medium uppercase text-[9px] md:text-xs flex items-center gap-0.5 md:gap-1 px-1.5 md:px-3 h-7 md:h-9"
                            >
                              <Pencil className="w-3 h-3" />
                              <span className="hidden sm:inline">Editar</span>
                            </Button>
                            <Button
                              size="sm"
                              variant="destructive"
                              onClick={() => handleDelete(partner.CODPARC)}
                              className="font-medium uppercase text-[9px] md:text-xs flex items-center gap-0.5 md:gap-1 px-1.5 md:px-3 h-7 md:h-9"
                              title="Inativar Cliente"
                            >
                              <Trash2 className="w-3 h-3" />
                              <span className="hidden sm:inline">Inativar</span>
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Pagination */}
          {!isLoading && partners.length > 0 && (
            <div className="flex flex-col items-center justify-center gap-3 bg-card rounded-lg shadow px-6 py-4">
              <div className="text-sm text-muted-foreground">
                Mostrando {startIndex + 1} a {endIndex} de {totalRecords} clientes
              </div>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handlePreviousPage}
                  disabled={currentPage === 1}
                  className="flex items-center gap-1"
                >
                  <ChevronLeft className="w-4 h-4" />
                  Anterior
                </Button>
                <div className="text-sm text-muted-foreground">
                  Página {currentPage} de {totalPages}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleNextPage}
                  disabled={currentPage === totalPages}
                  className="flex items-center gap-1"
                >
                  Próxima
                  <ChevronRight className="w-4 h-4" />
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <PartnerModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        partner={selectedPartner}
        onSave={handleSave}
        currentUser={currentUser}
      />
    </div>
  )
}