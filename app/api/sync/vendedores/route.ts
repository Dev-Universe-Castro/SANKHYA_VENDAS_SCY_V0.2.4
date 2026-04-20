
import { NextResponse } from 'next/server';
import {
  sincronizarVendedoresTotal,
  sincronizarVendedoresParcial,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarVendedores
} from '@/lib/sync-vendedores-service';
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
      // Ignora erro de parse se o corpo estiver vazio
    }

    const idSistema = idSistemaParam || body.idSistema;
    const empresa = empresaParam || body.empresa;
    const type = typeParam || body.type || 'total';

    if (idSistema && empresa) {
      console.log(`🔄 [API] Sincronizando vendedores: ${empresa} (ID: ${idSistema}) - Tipo: ${type}`);

      const resultado = type === 'partial'
        ? await sincronizarVendedoresParcial(parseInt(idSistema as string), empresa as string)
        : await sincronizarVendedoresTotal(parseInt(idSistema as string), empresa as string);

      return NextResponse.json(resultado);
    } else {
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar vendedores' },
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
      const data = await listarVendedores(idSistema ? Number(idSistema) : undefined);
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
