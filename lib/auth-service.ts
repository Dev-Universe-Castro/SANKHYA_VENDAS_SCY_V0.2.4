import type { User } from "./types"

// Simulate current logged-in user
let currentUser: User | null = null

// Super Admin padrão do sistema (não depende da API)
export const SUPER_ADMIN: User = {
  id: 0,
  name: "Super Admin",
  email: "sup@sankhya.com.br",
  password: "SUP321", // Senha em texto plano apenas para validação
  role: "Administrador",
  status: "ativo",
  avatar: ""
}

export const authService = {
  // Login user
  async login(email: string, password: string): Promise<User | null> {
    try {
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email, password }),
      });

      if (!response.ok) {
        return null;
      }

      const { user } = await response.json();

      if (user) {
        currentUser = user;
        // Store in localStorage for persistence
        if (typeof window !== "undefined") {
          localStorage.setItem("currentUser", JSON.stringify(user));
        }
        return user;
      }
      return null;
    } catch (error) {
      console.error('Login error:', error);
      return null;
    }
  },

  // Get current logged-in user
  getCurrentUser(): User | null {
    if (typeof window === 'undefined') return null

    try {
      console.log('🔍 getCurrentUser - Todos os cookies:', document.cookie)

      const userStr = document.cookie
        .split('; ')
        .find(row => row.startsWith('user='))
        ?.split('=')[1]

      console.log('🔍 getCurrentUser - Cookie encontrado:', !!userStr)

      if (!userStr) {
        console.log('❌ getCurrentUser - Nenhum cookie de usuário encontrado')
        return null
      }

      const user = JSON.parse(decodeURIComponent(userStr))
      console.log('✅ getCurrentUser - Usuário:', user.name)
      return user
    } catch (error) {
      console.error('❌ Erro ao obter usuário atual:', error)
      return null
    }
  },

  // Update current user profile
  async updateProfile(profileData: { name: string; email: string; avatar: string }) {
    try {
      const currentUser = this.getCurrentUser();
      if (!currentUser) {
        throw new Error("Usuário não autenticado");
      }

      const response = await fetch('/api/usuarios/salvar', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          userData: {
            id: currentUser.id,
            ...profileData,
            role: currentUser.role,
            status: currentUser.status,
            password: currentUser.password // Manter a senha atual
          },
          mode: 'edit' // Especificar que é uma edição
        }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Erro ao atualizar perfil');
      }

      const updatedUser = await response.json();

      // Atualizar usuário no localStorage
      const users = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
      const userIndex = users.findIndex((u: User) => u.id === updatedUser.id);
      if (userIndex !== -1) {
        users[userIndex] = updatedUser;
        localStorage.setItem(STORAGE_KEY, JSON.stringify(users));
      }

      return updatedUser;
    } catch (error) {
      console.error('Erro ao atualizar perfil:', error);
      throw error;
    }
  },

  // Logout user
  logout(): void {
    currentUser = null
    if (typeof window !== "undefined") {
      // Limpar localStorage
      localStorage.removeItem("currentUser")
      
      // Limpar sessionStorage (cache de prefetch)
      sessionStorage.removeItem('cached_parceiros')
      sessionStorage.removeItem('cached_produtos')
      
      // Remover cookie de usuário
      document.cookie = 'user=; path=/; max-age=0';
      
      // Limpar todo o sessionStorage para garantir
      sessionStorage.clear()
      
      console.log('🗑️ Cache de prefetch e usuário limpos')
    }
  },
}