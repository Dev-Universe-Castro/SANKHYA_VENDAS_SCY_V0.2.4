
import { NextResponse } from 'next/server';
import { consultarParceiros } from '@/lib/sankhya-api';
import { cacheService } from '@/lib/cache-service';
import { redisCacheService } from '@/lib/redis-cache-service';
import { cookies } from 'next/headers';

export const maxDuration = 30;

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q') || searchParams.get('search') || '';
    const limit = parseInt(searchParams.get('limit') || '50');

    // Validação
    if (query.length < 2) {
      return NextResponse.json(
        { parceiros: [], total: 0 },
        { 
          status: 200,
          headers: {
            'Cache-Control': 'no-store',
          }
        }
      );
    }

    console.log('🔍 Buscando parceiros com query:', query);

    // Obter filtros do usuário
    const cookieStore = cookies();
    const userCookie = cookieStore.get('user');
    let codVendedor: number | undefined;
    let codVendedoresEquipe: number[] | undefined;

    if (userCookie) {
      try {
        const user = JSON.parse(userCookie.value);
        const userCodVend = user.codVendedor ? parseInt(user.codVendedor) : null;
        
        if (user.role === 'Vendedor' && userCodVend) {
          codVendedor = userCodVend;
        }
        
        if (user.role === 'Gerente' && userCodVend) {
          const vendedoresResponse = await fetch(
            `${process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000'}/api/vendedores?tipo=vendedores&codGerente=${userCodVend}`
          );
          
          if (vendedoresResponse.ok) {
            const vendedores = await vendedoresResponse.json();
            if (vendedores && vendedores.length > 0) {
              codVendedoresEquipe = vendedores.map((v: any) => parseInt(v.CODVEND));
            } else {
              codVendedoresEquipe = [];
            }
          }
        }
      } catch (e) {
        console.error('Erro ao parsear cookie:', e);
      }
    }

    // Verificar cache específico da busca PRIMEIRO (mais preciso)
    const cacheKey = `search:parceiros:${query}:${limit}:${codVendedor}:${codVendedoresEquipe?.join(',')}`;
    
    // Tentar cache Redis primeiro
    const cachedRedis = await redisCacheService.get<any>(cacheKey);
    if (cachedRedis !== null) {
      console.log('✅ Retornando do cache Redis de busca específica');
      return NextResponse.json(cachedRedis, {
        headers: {
          'X-Cache': 'HIT-REDIS',
          'Cache-Control': 'public, max-age=180',
        },
      });
    }

    // Tentar cache em memória
    const cached = cacheService.get<any>(cacheKey);
    
    if (cached !== null) {
      console.log('✅ Retornando do cache de busca específica');
      return NextResponse.json(cached, {
        headers: {
          'X-Cache': 'HIT',
          'Cache-Control': 'public, max-age=180',
        },
      });
    }

    // Buscar parceiros da API
    console.log('📡 Buscando parceiros da API Sankhya...');
    const resultado = await consultarParceiros(
      1,
      limit,
      query,
      '',
      codVendedor,
      codVendedoresEquipe
    );
    
    // Salvar no cache Redis (10 minutos para buscas frequentes)
    await redisCacheService.set(cacheKey, resultado, 10 * 60 * 1000);
    // Salvar também no cache em memória como fallback
    cacheService.set(cacheKey, resultado, 10 * 60 * 1000);

    console.log(`✅ ${resultado.parceiros?.length || 0} parceiros encontrados e salvos no cache`);

    return NextResponse.json(resultado, {
      headers: {
        'X-Cache': 'MISS',
        'Cache-Control': 'public, s-maxage=600, stale-while-revalidate=900',
      },
    });
  } catch (error: any) {
    console.error('Erro na busca rápida de parceiros:', error);
    return NextResponse.json(
      { error: error.message || 'Erro na busca' },
      { status: 500 }
    );
  }
}
