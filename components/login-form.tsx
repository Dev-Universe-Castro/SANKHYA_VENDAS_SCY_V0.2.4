
"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { authService } from "@/lib/auth-service"
import { toast } from "@/components/ui/use-toast"
import { Eye, EyeOff } from "lucide-react"
import Image from "next/image"
// Prefetch removido - sistema multi-tenant
import { SplashScreen } from "@/components/splash-screen"

export default function LoginForm() {
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [showPassword, setShowPassword] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [showPrefetchSplash, setShowPrefetchSplash] = useState(false)
  const [isPrefetching, setIsPrefetching] = useState(false)
  const router = useRouter()

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      // Login usando endpoint de usuários FDV
      const response = await fetch('/api/auth/login-fdv', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email, password }),
      })

      const data = await response.json()

      if (response.ok && data.user) {
        // Salvar usuário no localStorage
        localStorage.setItem('user', JSON.stringify(data.user))

        toast({
          title: "Login realizado com sucesso!",
          description: `Bem-vindo(a), ${data.user.name}!`,
        })

        // Mostrar splash de prefetch
        setShowPrefetchSplash(true)
        setIsPrefetching(true)

        // Simular prefetch (sincronizador não precisa de muito prefetch)
        await new Promise(resolve => setTimeout(resolve, 1000))
        
        setIsPrefetching(false)

      } else {
        toast({
          title: "Erro no login",
          description: data.error || "Email ou senha inválidos, ou acesso não permitido.",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Erro no login",
        description: "Ocorreu um erro ao tentar fazer login.",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handlePrefetchFinish = () => {
    // Só redireciona quando o prefetch terminar
    if (!isPrefetching) {
      router.push("/dashboard")
    }
  }

  if (showPrefetchSplash) {
    return (
      <SplashScreen 
        onFinish={handlePrefetchFinish}
        duration={isPrefetching ? 15000 : 1000}
      />
    )
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4" style={{ backgroundColor: "oklch(0.32 0.02 235)" }}>
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-4">
          <div className="flex justify-center mb-4">
            <div className="relative w-64 h-20">
              <Image
                src="/sankhya-logo-horizontal.png"
                alt="Sankhya Logo"
                fill
                className="object-contain"
                priority
              />
            </div>
          </div>
          <CardTitle className="text-2xl text-center">Área de Login</CardTitle>
          <CardDescription className="text-center">
            Entre com suas credenciais para acessar o sistema
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                placeholder="seu@email.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">Senha</Label>
              <div className="relative">
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-700"
                >
                  {showPassword ? (
                    <EyeOff className="h-4 w-4" />
                  ) : (
                    <Eye className="h-4 w-4" />
                  )}
                </button>
              </div>
            </div>
            <Button type="submit" className="w-full" disabled={isLoading}>
              {isLoading ? "Entrando..." : "Entrar"}
            </Button>
          </form>
          <div className="mt-4 text-center text-sm text-muted-foreground">
            Acesso restrito a administradores
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
