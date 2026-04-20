
"use client"

import { useState, useEffect } from "react"
import { Search, Pencil, Trash2, Plus } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import DashboardLayout from "@/components/dashboard-layout"

interface UsuarioFDV {
  CODUSUARIO: number
  ID_EMPRESA: number
  NOME: string
  EMAIL: string
  SENHA?: string
  FUNCAO: string
  STATUS: 'ativo' | 'pendente' | 'bloqueado'
  AVATAR?: string
  DATACRIACAO: Date
  DATAATUALIZACAO: Date
  CODVEND?: number
  EMPRESA?: string
  CNPJ?: string
}

interface Contrato {
  ID_EMPRESA: number
  EMPRESA: string
  CNPJ: string
}

export default function UsuariosFDVPage() {
  const [usuarios, setUsuarios] = useState<UsuarioFDV[]>([])
  const [contratos, setContratos] = useState<Contrato[]>([])
  const [filteredUsuarios, setFilteredUsuarios] = useState<UsuarioFDV[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [empresaFiltro, setEmpresaFiltro] = useState<string>("todas")
  const [isLoading, setIsLoading] = useState(true)
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [modalMode, setModalMode] = useState<"create" | "edit">("create")
  const [selectedUsuario, setSelectedUsuario] = useState<UsuarioFDV | null>(null)
  const [formData, setFormData] = useState({
    ID_EMPRESA: 0,
    NOME: "",
    EMAIL: "",
    SENHA: "",
    FUNCAO: "Vendedor",
    STATUS: "ativo" as 'ativo' | 'pendente' | 'bloqueado',
    AVATAR: "",
    CODVEND: undefined as number | undefined
  })

  useEffect(() => {
    loadContratos()
    loadUsuarios()
  }, [])

  useEffect(() => {
    let filtered = usuarios

    if (searchTerm.trim() !== "") {
      filtered = filtered.filter(
        (user) =>
          user.NOME.toLowerCase().includes(searchTerm.toLowerCase()) ||
          user.EMAIL.toLowerCase().includes(searchTerm.toLowerCase()) ||
          user.FUNCAO.toLowerCase().includes(searchTerm.toLowerCase()) ||
          (user.EMPRESA && user.EMPRESA.toLowerCase().includes(searchTerm.toLowerCase()))
      )
    }

    if (empresaFiltro !== "todas") {
      filtered = filtered.filter((user) => user.ID_EMPRESA === parseInt(empresaFiltro))
    }

    setFilteredUsuarios(filtered)
  }, [searchTerm, empresaFiltro, usuarios])

  const loadContratos = async () => {
    try {
      const response = await fetch('/api/contratos')
      if (!response.ok) throw new Error('Erro ao carregar contratos')
      const data = await response.json()
      setContratos(data)
    } catch (error) {
      console.error("Erro ao carregar contratos:", error)
    }
  }

  const loadUsuarios = async () => {
    setIsLoading(true)
    try {
      const response = await fetch('/api/usuarios-fdv')
      if (!response.ok) throw new Error('Erro ao carregar usuários')
      const data = await response.json()
      setUsuarios(data)
      setFilteredUsuarios(data)
    } catch (error) {
      console.error("Erro ao carregar usuários:", error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleCreate = () => {
    setSelectedUsuario(null)
    setFormData({
      ID_EMPRESA: contratos.length > 0 ? contratos[0].ID_EMPRESA : 0,
      NOME: "",
      EMAIL: "",
      SENHA: "",
      FUNCAO: "Vendedor",
      STATUS: "ativo",
      AVATAR: "",
      CODVEND: undefined
    })
    setModalMode("create")
    setIsModalOpen(true)
  }

  const handleEdit = (usuario: UsuarioFDV) => {
    setSelectedUsuario(usuario)
    setFormData({
      ID_EMPRESA: usuario.ID_EMPRESA,
      NOME: usuario.NOME,
      EMAIL: usuario.EMAIL,
      SENHA: "",
      FUNCAO: usuario.FUNCAO,
      STATUS: usuario.STATUS,
      AVATAR: usuario.AVATAR || "",
      CODVEND: usuario.CODVEND
    })
    setModalMode("edit")
    setIsModalOpen(true)
  }

  const handleDelete = async (id: number) => {
    if (!confirm("Tem certeza que deseja deletar este usuário?")) return

    try {
      const response = await fetch(`/api/usuarios-fdv/${id}`, {
        method: 'DELETE'
      })
      if (!response.ok) throw new Error('Erro ao deletar usuário')
      await loadUsuarios()
    } catch (error) {
      console.error("Erro ao deletar usuário:", error)
      alert('Erro ao deletar usuário')
    }
  }

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault()

    try {
      if (modalMode === "create") {
        const response = await fetch('/api/usuarios-fdv', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formData)
        })
        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Erro ao criar usuário')
        }
      } else {
        const response = await fetch(`/api/usuarios-fdv/${selectedUsuario?.CODUSUARIO}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formData)
        })
        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Erro ao atualizar usuário')
        }
      }

      setIsModalOpen(false)
      await loadUsuarios()
    } catch (error: any) {
      console.error("Erro ao salvar usuário:", error)
      alert(error.message || 'Erro ao salvar usuário')
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "ativo":
        return <Badge className="bg-green-500 hover:bg-green-600">Ativo</Badge>
      case "pendente":
        return <Badge className="bg-yellow-500 hover:bg-yellow-600">Pendente</Badge>
      case "bloqueado":
        return <Badge className="bg-red-500 hover:bg-red-600">Bloqueado</Badge>
      default:
        return <Badge>{status}</Badge>
    }
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-foreground">Usuários FDV</h1>
            <p className="text-sm text-muted-foreground">
              Gerenciamento de usuários do aplicativo FDV por empresa
            </p>
          </div>
          <Button onClick={handleCreate} className="bg-primary hover:bg-primary/90">
            <Plus className="w-4 h-4 mr-2" />
            Novo Usuário
          </Button>
        </div>

        {/* Filters */}
        <div className="flex gap-4">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input
              type="text"
              placeholder="Buscar por nome, email, função ou empresa..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 bg-card"
            />
          </div>
          <Select value={empresaFiltro} onValueChange={setEmpresaFiltro}>
            <SelectTrigger className="w-[250px]">
              <SelectValue placeholder="Filtrar por empresa" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todas">Todas as empresas</SelectItem>
              {contratos.map((contrato) => (
                <SelectItem key={contrato.ID_EMPRESA} value={String(contrato.ID_EMPRESA)}>
                  {contrato.EMPRESA}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Table */}
        <div className="bg-card rounded-lg shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="sticky top-0 z-10" style={{ backgroundColor: 'rgb(35, 55, 79)' }}>
                <tr>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    ID
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Empresa
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Nome
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Email
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Função
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-4 text-left text-sm font-semibold text-white uppercase tracking-wider">
                    Ações
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {isLoading ? (
                  <tr>
                    <td colSpan={7} className="px-6 py-8 text-center text-muted-foreground">
                      Carregando...
                    </td>
                  </tr>
                ) : filteredUsuarios.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-6 py-8 text-center text-muted-foreground">
                      Nenhum usuário encontrado
                    </td>
                  </tr>
                ) : (
                  filteredUsuarios.map((usuario) => (
                    <tr key={usuario.CODUSUARIO} className="hover:bg-muted/50 transition-colors">
                      <td className="px-6 py-4 text-sm text-foreground">{usuario.CODUSUARIO}</td>
                      <td className="px-6 py-4 text-sm text-foreground">
                        <div>
                          <div className="font-medium">{usuario.EMPRESA}</div>
                          <div className="text-xs text-muted-foreground">{usuario.CNPJ}</div>
                        </div>
                      </td>
                      <td className="px-6 py-4 text-sm text-foreground">{usuario.NOME}</td>
                      <td className="px-6 py-4 text-sm text-foreground">{usuario.EMAIL}</td>
                      <td className="px-6 py-4 text-sm">
                        <Badge variant="secondary">{usuario.FUNCAO}</Badge>
                      </td>
                      <td className="px-6 py-4 text-sm">{getStatusBadge(usuario.STATUS)}</td>
                      <td className="px-6 py-4">
                        <div className="flex gap-2">
                          <Button
                            size="sm"
                            onClick={() => handleEdit(usuario)}
                            className="bg-primary hover:bg-primary/90 text-primary-foreground"
                          >
                            <Pencil className="w-3 h-3 mr-1" />
                            Editar
                          </Button>
                          <Button
                            size="sm"
                            variant="destructive"
                            onClick={() => handleDelete(usuario.CODUSUARIO)}
                          >
                            <Trash2 className="w-3 h-3 mr-1" />
                            Deletar
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

        {/* Modal */}
        <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>
                {modalMode === "create" ? "Criar Novo Usuário FDV" : "Editar Usuário FDV"}
              </DialogTitle>
            </DialogHeader>
            <form onSubmit={handleSave} className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <Label htmlFor="empresa">Empresa *</Label>
                  <Select
                    value={String(formData.ID_EMPRESA)}
                    onValueChange={(value) => setFormData({ ...formData, ID_EMPRESA: parseInt(value) })}
                    disabled={modalMode === "edit"}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Selecione a empresa" />
                    </SelectTrigger>
                    <SelectContent>
                      {contratos.map((contrato) => (
                        <SelectItem key={contrato.ID_EMPRESA} value={String(contrato.ID_EMPRESA)}>
                          {contrato.EMPRESA}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="col-span-2">
                  <Label htmlFor="nome">Nome *</Label>
                  <Input
                    id="nome"
                    value={formData.NOME}
                    onChange={(e) => setFormData({ ...formData, NOME: e.target.value })}
                    required
                  />
                </div>

                <div className="col-span-2">
                  <Label htmlFor="email">Email *</Label>
                  <Input
                    id="email"
                    type="email"
                    value={formData.EMAIL}
                    onChange={(e) => setFormData({ ...formData, EMAIL: e.target.value })}
                    required
                  />
                </div>

                <div className="col-span-2">
                  <Label htmlFor="senha">
                    Senha {modalMode === "create" ? "*" : "(deixe em branco para manter)"}
                  </Label>
                  <Input
                    id="senha"
                    type="password"
                    value={formData.SENHA}
                    onChange={(e) => setFormData({ ...formData, SENHA: e.target.value })}
                    required={modalMode === "create"}
                  />
                </div>

                <div>
                  <Label htmlFor="funcao">Função *</Label>
                  <Select
                    value={formData.FUNCAO}
                    onValueChange={(value) => setFormData({ ...formData, FUNCAO: value })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Vendedor">Vendedor</SelectItem>
                      <SelectItem value="Gerente">Gerente</SelectItem>
                      <SelectItem value="Administrador">Administrador</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label htmlFor="status">Status *</Label>
                  <Select
                    value={formData.STATUS}
                    onValueChange={(value: any) => setFormData({ ...formData, STATUS: value })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="ativo">Ativo</SelectItem>
                      <SelectItem value="pendente">Pendente</SelectItem>
                      <SelectItem value="bloqueado">Bloqueado</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="col-span-2">
                  <Label htmlFor="avatar">Avatar URL</Label>
                  <Input
                    id="avatar"
                    type="url"
                    value={formData.AVATAR}
                    onChange={(e) => setFormData({ ...formData, AVATAR: e.target.value })}
                    placeholder="https://exemplo.com/avatar.jpg"
                  />
                </div>
              </div>

              <DialogFooter>
                <Button type="button" variant="outline" onClick={() => setIsModalOpen(false)}>
                  Cancelar
                </Button>
                <Button type="submit" className="bg-primary hover:bg-primary/90">
                  {modalMode === "create" ? "Criar" : "Salvar"}
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}
