
import { NextResponse, NextRequest } from 'next/server';
import {
  sincronizarProdutosTotal,
  sincronizarProdutosParcial,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarProdutos
} from '@/lib/sync-produtos-service';

export async function POST(request: NextRequest) {
  try {
    // 1. Tentar obter parâmetros da URL (Query String)
    const { searchParams } = new URL(request.url);
    let idSistema = searchParams.get('idSistema');
    let empresa = searchParams.get('empresa');
    let type = searchParams.get('type');
    let startPage = searchParams.get('startPage');

    // 2. Tentar obter parâmetros do corpo (JSON)
    try {
      if (request.body) {
         const body = await request.json();
         if (body) {
           if (!idSistema && body.idSistema) idSistema = body.idSistema;
           if (!empresa && body.empresa) empresa = body.empresa;
           if (!type && body.type) type = body.type;
           if (!startPage && body.startPage) startPage = body.startPage;
         }
      }
    } catch (e) {
      // Ignorar erro se o corpo não for JSON
    }

    const typeFinal = type || 'total';

    if (idSistema && empresa) {
      // Sincronizar empresa específica
      console.log(`🔄 [API] Sincronizando empresa: ${empresa} (ID: ${idSistema}) - Tipo: ${typeFinal}${startPage !== undefined ? ` - Página: ${startPage}` : ''}`);

      let resultado;
      if (startPage !== undefined) {
        const { sincronizarProdutosRetomada } = await import('@/lib/sync-produtos-service');
        resultado = await sincronizarProdutosRetomada(parseInt(idSistema), empresa as string, Number(startPage));
      } else {
        resultado = typeFinal === 'partial'
          ? await sincronizarProdutosParcial(parseInt(idSistema), empresa as string)
          : await sincronizarProdutosTotal(parseInt(idSistema), empresa as string);
      }

      return NextResponse.json(resultado);
    } else {
      // Sincronizar todas as empresas (uma por vez)
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar produtos' },
      { status: 500 }
    );
  }
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const idSistema = searchParams.get('idSistema');
    const list = searchParams.get('list');

    if (list === 'true') {
      const data = await listarProdutos(idSistema ? Number(idSistema) : undefined);
      return NextResponse.json(data);
    }

    const estatisticas = await obterEstatisticasSincronizacao(
      idSistema ? parseInt(idSistema) : undefined
    );

    return NextResponse.json(estatisticas);
  } catch (error: any) {
    console.error('❌ Erro ao obter estatísticas:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
