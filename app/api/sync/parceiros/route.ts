
import { NextResponse } from 'next/server';
import {
  sincronizarParceirosTotal,
  sincronizarParceirosParcial,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarParceiros
} from '@/lib/sync-parceiros-service';
import { NextRequest } from 'next/server';

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { idSistema, empresa, type = 'total' } = body;

    if (idSistema && empresa) {
      // Sincronizar empresa específica
      console.log(`🔄 [API] Sincronizando parceiros: ${empresa} (ID: ${idSistema}) - Tipo: ${type}`);

      const resultado = type === 'partial'
        ? await sincronizarParceirosParcial(parseInt(idSistema), empresa)
        : await sincronizarParceirosTotal(parseInt(idSistema), empresa);

      return NextResponse.json(resultado);
    } else {
      // Sincronizar todas as empresas
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar parceiros' },
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
      const data = await listarParceiros(idSistema ? Number(idSistema) : undefined);
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
