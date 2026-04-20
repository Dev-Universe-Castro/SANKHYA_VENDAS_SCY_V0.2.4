
import { NextResponse } from 'next/server';
import {
  sincronizarTabelaPrecosTotal,
  sincronizarTabelaPrecosParcial,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarTabelaPrecos
} from '@/lib/sync-tabelas-precos-service';
import { NextRequest } from 'next/server';

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistemaParam = searchParams.get('idSistema');
    const empresaParam = searchParams.get('empresa');
    const typeParam = searchParams.get('type');

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

    if (idSistema && empresa) {
      // Sincronizar empresa específica
      console.log(`🔄 [API] Sincronizando tabelas de preços: ${empresa} (ID: ${idSistema}) - Tipo: ${type}`);

      const resultado = type === 'partial'
        ? await sincronizarTabelaPrecosParcial(parseInt(idSistema as string), empresa as string)
        : await sincronizarTabelaPrecosTotal(parseInt(idSistema as string), empresa as string);

      return NextResponse.json(resultado);
    } else {
      // Sincronizar todas as empresas (uma por vez)
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar tabelas de preços' },
      { status: 500 }
    );
  }
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const list = searchParams.get('list');

    if (list === 'true') {
      const data = await listarTabelaPrecos(idSistema ? Number(idSistema) : undefined);
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
