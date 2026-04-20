
import { NextResponse } from 'next/server';
import { getCacheService } from '@/lib/redis-cache-wrapper';

const USERS_ONLINE_KEY = 'global:users:online';
const ONLINE_TIMEOUT = 5 * 60 * 1000; // 5 minutos

export async function GET() {
  try {
    const cacheService = await getCacheService();
    const usersOnline = await cacheService.get<any[]>(USERS_ONLINE_KEY) || [];
    
    // Filtrar usuários que ainda estão online (últimos 5 minutos)
    const now = Date.now();
    const activeUsers = usersOnline.filter(user => {
      return (now - new Date(user.lastActivity).getTime()) < ONLINE_TIMEOUT;
    });

    // Atualizar cache removendo usuários inativos
    if (activeUsers.length !== usersOnline.length) {
      await cacheService.set(USERS_ONLINE_KEY, activeUsers, 24 * 60 * 60 * 1000);
    }

    return NextResponse.json({
      users: activeUsers,
      total: activeUsers.length
    });
  } catch (error) {
    console.error('❌ Erro ao obter usuários online:', error);
    return NextResponse.json({
      error: 'Erro ao obter usuários online',
      users: [],
      total: 0
    }, { status: 500 });
  }
}

// Atualizar atividade do usuário
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { userId, userName, email } = body;

    if (!userId) {
      return NextResponse.json({
        error: 'userId é obrigatório'
      }, { status: 400 });
    }

    const cacheService = await getCacheService();
    const usersOnline = await cacheService.get<any[]>(USERS_ONLINE_KEY) || [];
    
    // Remover entrada antiga do usuário
    const filteredUsers = usersOnline.filter(u => u.userId !== userId);
    
    // Adicionar entrada atualizada
    filteredUsers.push({
      userId,
      userName: userName || 'Usuário',
      email: email || '',
      lastActivity: new Date().toISOString()
    });

    // Manter apenas últimos 100 usuários
    const updatedUsers = filteredUsers.slice(-100);
    
    await cacheService.set(USERS_ONLINE_KEY, updatedUsers, 24 * 60 * 60 * 1000);

    return NextResponse.json({
      success: true
    });
  } catch (error) {
    console.error('❌ Erro ao atualizar atividade do usuário:', error);
    return NextResponse.json({
      error: 'Erro ao atualizar atividade'
    }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';
