"use client"

import Link from "next/link"
import { usePathname, useRouter } from "next/navigation"
import { Home, Users, ChevronLeft, ChevronRight, LogOut, UserCircle, LayoutGrid, Package, ShoppingCart, Calendar, DollarSign, RefreshCw, FileText, Clock, Activity } from "lucide-react"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import Image from "next/image"
import { authService } from "@/lib/auth-service"
import { useState, useEffect } from "react"
import type { User } from "@/lib/users-service"

interface SidebarProps {
  isOpen: boolean
  onClose: () => void
  isCollapsed: boolean
  onToggleCollapse: () => void
}

export default function Sidebar({ isOpen, onClose, isCollapsed, onToggleCollapse }: SidebarProps) {
  const [currentUser, setCurrentUser] = useState<User | null>(null)
  const pathname = usePathname()
  const router = useRouter()

  useEffect(() => {
    setCurrentUser(authService.getCurrentUser())
  }, [])

  const menuItems = [
    { href: "/dashboard", label: "Início", icon: Home },
    ...(currentUser?.role === "Administrador"
      ? [
        { href: "/dashboard/contratos", label: "Contratos", icon: Package },
        { href: "/dashboard/usuarios-fdv", label: "Usuários FDV", icon: Users }
      ]
      : []),
    ...(currentUser?.role === "Gerente"
      ? [
        { href: "/dashboard/equipe", label: "Equipe Comercial", icon: Users },
        { href: "/dashboard/usuarios-fdv", label: "Usuários FDV", icon: UserCircle }
      ]
      : []),
    // Links de sincronização (apenas para administradores)
    ...(currentUser?.role === "Administrador"
      ? [
        { href: "/dashboard/agendamentos", label: "Agendamentos", icon: Clock },
        { href: "/dashboard/sincronizacao", label: "Sinc. Parceiros", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-produtos", label: "Sinc. Produtos", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-volumes-alternativos", label: "Sinc. Volumes Alt.", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-marcas", label: "Sinc. Marcas", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-grupos-produtos", label: "Sinc. Grupos Produtos", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-bairros", label: "Sinc. Bairros", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-cidades", label: "Sinc. Cidades", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-empresas", label: "Sinc. Empresas", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-regioes", label: "Sinc. Regiões", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-estados", label: "Sinc. Estados", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-tipos-negociacao", label: "Sinc. Tipos Negociação", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-tipos-operacao", label: "Sinc. Tipos Operação", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-estoques", label: "Sinc. Estoques", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-tabela-precos", label: "Sinc. Tabela Preços", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-excecao-preco", label: "Sinc. Exceção Preço", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-parceiro-empres-grupo-icms", label: "Sinc. Regras ICMS Parc.", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-vendedores", label: "Sinc. Vendedores", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-complemento-parceiro", label: "Sinc. Comp. Parceiro", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-cabecalho-nota", label: "Sinc. Cabeçalhos de Nota", icon: RefreshCw },
        { href: "/dashboard/sincronizacao-itens-nota", label: "Sinc. Itens de Nota", icon: RefreshCw },
        { href: "/dashboard/logs-sincronizacao", label: "Logs de Sinc.", icon: LayoutGrid },
      ]
      : [])
  ]

  const handleLogout = () => {
    authService.logout()
    router.push("/")
  }

  return (
    <>
      <aside
        className={cn(
          "fixed lg:static inset-y-0 left-0 z-30 bg-sidebar transform transition-all duration-200 ease-in-out flex flex-col min-w-[256px]",
          isCollapsed ? "w-20 min-w-[80px]" : "w-64",
          isOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0",
        )}
      >
        {/* Logo */}
        <div
          className={cn(
            "border-b border-sidebar-border flex items-center justify-between shrink-0",
            isCollapsed ? "p-4" : "p-6",
          )}
        >
          <div className={cn("flex items-center", isCollapsed ? "justify-center w-full" : "gap-3")}>
            {isCollapsed ? (
              <Image src="/sankhya-icon.jpg" alt="Sankhya" width={40} height={40} className="object-contain" />
            ) : (
              <Image
                src="/sankhya-logo-horizontal.png"
                alt="Sankhya"
                width={180}
                height={60}
                className="object-contain"
              />
            )}
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={onToggleCollapse}
            className="hidden lg:flex text-sidebar-foreground hover:bg-sidebar-accent absolute -right-3 top-6 bg-sidebar border border-sidebar-border rounded-full w-6 h-6 p-0"
          >
            {isCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
          </Button>
        </div>

        {/* Menu section */}
        <div className="flex-1 p-4 overflow-y-auto">
          {!isCollapsed && (
            <p className="text-xs uppercase tracking-wider text-sidebar-foreground/60 mb-3 px-3">Expedição</p>
          )}
          <nav className="space-y-1">
            {menuItems.map((item) => {
              const Icon = item.icon
              const isActive = pathname === item.href
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  prefetch={false} // Otimização: desabilitar prefetch automático para links não essenciais
                  onClick={onClose}
                  className={cn(
                    "flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap",
                    isCollapsed && "justify-center",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground/80 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground",
                  )}
                  title={isCollapsed ? item.label : undefined}
                >
                  <Icon className="w-5 h-5 flex-shrink-0" />
                  {!isCollapsed && <span className="truncate">{item.label}</span>}
                </Link>
              )
            })}
          </nav>
        </div>

        <div className="p-4 border-t border-sidebar-border shrink-0">
          <Button
            variant="ghost"
            onClick={handleLogout}
            className={cn(
              "w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-sidebar-foreground/80 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground whitespace-nowrap",
              isCollapsed && "justify-center px-0",
            )}
            title={isCollapsed ? "Sair" : undefined}
          >
            <LogOut className="w-5 h-5 flex-shrink-0" />
            {!isCollapsed && <span className="truncate">Sair</span>}
          </Button>
        </div>
      </aside>
    </>
  )
}