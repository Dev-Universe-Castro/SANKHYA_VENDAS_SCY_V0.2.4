import { NextRequest, NextResponse } from 'next/server';
import { sincronizarVolumesAlternativosTotal } from '@/lib/sync-volumes-alternativos-service';

export async function POST(request: NextRequest) {
  try {
    // 1. Tentar obter parâmetros da URL (Query String)
    const { searchParams } = new URL(request.url);
    let idSistema = searchParams.get('idSistema');
    let empresa = searchParams.get('empresa');
    let type = searchParams.get('type');

    // 2. Tentar obter parâmetros do corpo (JSON)
    try {
      const body = await request.json();
      if (body) {
        if (!idSistema && body.idSistema) idSistema = body.idSistema;
        if (!empresa && body.empresa) empresa = body.empresa;
        if (!type && body.type) type = body.type;
      }
    } catch (e) {
      // Ignorar erro se o corpo não for JSON (pode ser requisição via query string)
      console.log('ℹ️ [API] Corpo da requisição não é JSON ou está vazio');
    }

    if (!idSistema) {
      return NextResponse.json({ error: 'idSistema é obrigatório' }, { status: 400 });
    }

    const idSistemaNum = parseInt(idSistema as string);
    const empresaNome = (empresa as string) || 'Empresa';

    console.log(`🚀 [API] Disparando sincronização de Volumes Alternativos para ID: ${idSistemaNum} (${empresaNome})`);

    const result = await sincronizarVolumesAlternativosTotal(idSistemaNum, empresaNome);

    if (result.success) {
      return NextResponse.json(result);
    } else {
      return NextResponse.json(result, { status: 500 });
    }
  } catch (error: any) {
    console.error('❌ [API] Erro na rota de sincronização de Volumes Alternativos:', error);
    return NextResponse.json({ 
      success: false, 
      erro: error.message 
    }, { status: 500 });
  }
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const list = searchParams.get('list');

    const idSistemaNum = idSistema ? parseInt(idSistema) : undefined;

    const { obterEstatisticasSincronizacao, listarVolumes } = await import('@/lib/sync-volumes-alternativos-service');

    if (list === 'true') {
      const volumes = await listarVolumes(idSistemaNum);
      return NextResponse.json(volumes);
    } else {
      const stats = await obterEstatisticasSincronizacao(idSistemaNum);
      return NextResponse.json(stats);
    }
  } catch (error: any) {
    console.error('❌ [API] Erro ao buscar dados de Volumes Alternativos:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
