import { NextResponse } from 'next/server';
import { obterToken } from '@/lib/sankhya-api';
import { redisCacheService } from '@/lib/redis-cache-service';

export async function POST() {
  try {
    console.log('🔄 [API /admin/refresh-token] Forçando renovação do token do servidor...');

    // Limpar cache do token antes de renovar
    await redisCacheService.delete('sankhya:token');
    console.log('🗑️ [API /admin/refresh-token] Cache do token limpo');

    // Forçar geração de novo token
    const novoToken = await obterToken(true);

    console.log('✅ [API /admin/refresh-token] Token do servidor renovado com sucesso');

    return NextResponse.json({ 
      success: true,
      message: 'Token do servidor renovado com sucesso',
      token: novoToken.substring(0, 20) + '...' // Mostrar apenas início do token
    });
  } catch (erro: any) {
    console.error('❌ [API /admin/refresh-token] Erro ao renovar token:', erro.message);
    return NextResponse.json({
      success: false,
      error: erro.message
    }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';