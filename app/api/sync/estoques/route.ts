
import { NextRequest, NextResponse } from 'next/server';
import {
  sincronizarEstoquesPorEmpresa,
  sincronizarEstoquesParcial,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarEstoques
} from '@/lib/sync-estoques-service';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const list = searchParams.get('list');

    if (list === 'true') {
      const data = await listarEstoques(idSistema ? Number(idSistema) : undefined);
      return NextResponse.json(data);
    }

    if (idSistema) {
      const stats = await obterEstatisticasSincronizacao(parseInt(idSistema));
      return NextResponse.json(stats);
    }

    const stats = await obterEstatisticasSincronizacao();
    return NextResponse.json(stats);
  } catch (error: any) {
    console.error('Erro ao obter estatísticas:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistemaParam = searchParams.get('idSistema');
    const empresaParam = searchParams.get('empresa');
    const typeParam = searchParams.get('type');
    const startPageParam = searchParams.get('startPage');

    let body: any = {};
    try {
      const text = await request.text();
      if (text) {
        body = JSON.parse(text);
      }
    } catch (e) {
      // Ignora erro se corpo estiver vazio
    }

    const idSistema = idSistemaParam || body.idSistema;
    const empresa = empresaParam || body.empresa;
    const type = typeParam || body.type || 'total';
    const startPage = startPageParam || body.startPage;

    console.log('📥 [API] Requisição de sincronização recebida:', { idSistema, empresa, type, startPage });

    if (idSistema && empresa) {
      console.log(`🔄 [API] Sincronizando empresa: ${empresa} (ID: ${idSistema}) - Tipo: ${type}${startPage ? ` - Página: ${startPage}` : ''}`);
      let resultado;

      if (startPage) {
        const { sincronizarEstoquesRetomada } = await import('@/lib/sync-estoques-service');
        resultado = await sincronizarEstoquesRetomada(parseInt(idSistema as string), empresa as string, Number(startPage));
      } else if (type === 'partial') {
        resultado = await sincronizarEstoquesParcial(parseInt(idSistema as string), empresa as string);
      } else {
        resultado = await sincronizarEstoquesPorEmpresa(parseInt(idSistema as string), empresa as string);
      }
      return NextResponse.json(resultado);
    }

    console.log('🔄 [API] Sincronizando todas as empresas');
    const resultados = await sincronizarTodasEmpresas();
    return NextResponse.json(resultados);
  } catch (error: any) {
    console.error('❌ [API] Erro ao sincronizar estoques:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar estoques' },
      { status: 500 }
    );
  }
}
