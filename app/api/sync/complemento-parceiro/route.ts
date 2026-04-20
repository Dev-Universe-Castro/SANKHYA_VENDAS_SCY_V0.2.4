import { NextResponse } from 'next/server';
import {
  sincronizarComplementoParcTotal,
  sincronizarComplementoParcParcial,
  obterEstatisticasSincronizacao
} from '@/lib/sync-complemento-parc-service';
import { NextRequest } from 'next/server';

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { idSistema, empresa, type = 'total' } = body;

    if (idSistema && empresa) {
      // Sincronizar empresa específica
      console.log(`🔄 [API] Sincronizando complemento de parceiro: ${empresa} (ID: ${idSistema}) - Tipo: ${type}`);

      const resultado = type === 'partial'
        ? await sincronizarComplementoParcParcial(parseInt(idSistema), empresa)
        : await sincronizarComplementoParcTotal(parseInt(idSistema), empresa);

      return NextResponse.json(resultado);
    } else {
      // Endpoint to sync all is not completely implemented here since the queue handles it generally,
      // but returning a mock success if asked.
      return NextResponse.json({ success: false, erro: 'Use a fila global' });
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar' },
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
      // Retorna array vazio por enquanto pois não fiz o listarComplementos no momento e não há UX da tabela completa ainda
      return NextResponse.json([]);
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
