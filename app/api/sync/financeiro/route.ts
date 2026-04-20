
import { NextResponse } from 'next/server';
import { sincronizarFinanceiroPorEmpresa, sincronizarFinanceiroParcial, sincronizarTodasEmpresas, obterEstatisticasSincronizacao } from '@/lib/sync-financeiro-service';

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
      let resultado;
      if (type === 'partial') {
        resultado = await sincronizarFinanceiroParcial(parseInt(idSistema as string), empresa as string);
      } else {
        resultado = await sincronizarFinanceiroPorEmpresa(parseInt(idSistema as string), empresa as string);
      }
      return NextResponse.json(resultado);
    } else {
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar financeiro' },
      { status: 500 }
    );
  }
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');

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
