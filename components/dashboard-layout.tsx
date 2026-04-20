"use client"

import type React from "react"

import { useState } from "react"
import Sidebar from "@/components/sidebar"
import Header from "@/components/header"
import Footer from "@/components/footer"
import FloatingActionMenu from "@/components/floating-action-menu"
import { useToast } from "@/components/ui/use-toast"
import { authService } from "@/lib/auth-service"

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const { toast } = useToast()

  const handleLogout = async () => {
    authService.logout()

    // Limpar cache do servidor também
    try {
      await fetch('/api/cache/clear?userLogout=true', {
        method: 'POST',
      })
      console.log('✅ Cache do servidor limpo')
    } catch (error) {
      console.error('Erro ao limpar cache do servidor:', error)
    }

    toast({
      title: "Logout realizado",
      description: "Você foi desconectado com sucesso.",
    })

    // Redirecionar para login após pequeno delay
    setTimeout(() => {
      window.location.href = '/'
    }, 500)
  }

  return (
    <div className="min-h-screen flex flex-col">
      <div className="flex flex-1">
        {/* Mobile sidebar backdrop */}
        {sidebarOpen && (
          <div className="fixed inset-0 bg-black/50 z-20 lg:hidden" onClick={() => setSidebarOpen(false)} />
        )}

        {/* Sidebar */}
        <Sidebar
          isOpen={sidebarOpen}
          onClose={() => setSidebarOpen(false)}
          isCollapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        />

        {/* Main content */}
        <div className="flex-1 flex flex-col overflow-x-hidden">
          <Header onMenuClick={() => setSidebarOpen(!sidebarOpen)} onLogout={handleLogout} />
          <main className="flex-1 p-6 bg-background pb-20 lg:pb-0 overflow-x-hidden page-transition">
            {children}
          </main>
          <Footer />
        </div>
      </div>

      {/* Floating Action Menu */}
      {/* FloatingActionMenu removed as per instruction */}
    </div>
  )
}