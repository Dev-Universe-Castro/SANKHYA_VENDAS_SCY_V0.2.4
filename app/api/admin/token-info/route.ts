import { NextResponse } from 'next/server';
import { getCacheService } from '@/lib/redis-cache-wrapper';

export async function GET() {
  try {
    const cacheService = await getCacheService();
    const tokenData = await cacheService.get<any>('sankhya:token');

    if (!tokenData) {
      return NextResponse.json({
        hasToken: false,
        message: 'Nenhum token ativo'
      });
    }

    const now = Date.now();
    const expiresAt = tokenData.expiresAt || 0;
    const timeRemaining = Math.max(0, expiresAt - now);
    const minutesRemaining = Math.floor(timeRemaining / 60000);
    const isExpired = timeRemaining <= 0;

    return NextResponse.json({
      hasToken: true,
      isExpired,
      expiresAt: new Date(expiresAt).toISOString(),
      timeRemaining: timeRemaining,
      minutesRemaining,
      createdAt: tokenData.createdAt ? new Date(tokenData.createdAt).toISOString() : null
    });
  } catch (error) {
    console.error('❌ Erro ao obter informações do token:', error);
    return NextResponse.json({
      error: 'Erro ao obter informações do token'
    }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';