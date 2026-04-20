
import { NextResponse } from 'next/server';
import { redisCacheService } from '@/lib/redis-cache-service';

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const pattern = searchParams.get('pattern');
    const userLogout = searchParams.get('userLogout');

    // Se for logout de usuário, limpar caches críticos
    if (userLogout === 'true') {
      console.log('🗑️ Limpando cache de sessão do usuário...');
      await redisCacheService.invalidatePattern('parceiros');
      await redisCacheService.invalidatePattern('produtos');
      await redisCacheService.invalidatePattern('preco');
      await redisCacheService.invalidatePattern('estoque');
      return NextResponse.json({ 
        success: true, 
        message: 'Cache de sessão limpo com sucesso' 
      });
    }

    if (pattern) {
      const count = await redisCacheService.invalidatePattern(pattern);
      return NextResponse.json({ 
        success: true, 
        message: `Cache limpo: ${count} registros com padrão '${pattern}'` 
      });
    } else {
      await redisCacheService.clear();
      return NextResponse.json({ 
        success: true, 
        message: 'Cache completamente limpo' 
      });
    }
  } catch (error: any) {
    console.error('Erro ao limpar cache:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao limpar cache' },
      { status: 500 }
    );
  }
}

export async function GET(request: Request) {
  try {
    const stats = await redisCacheService.getStats();
    return NextResponse.json({ 
      success: true, 
      stats 
    });
  } catch (error: any) {
    console.error('Erro ao obter estatísticas do cache:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}
