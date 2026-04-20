
import { NextResponse } from 'next/server';
import { syncQueueService } from '@/lib/sync-queue-service';

export async function GET() {
  try {
    const status = syncQueueService.getQueueStatus();
    return NextResponse.json(status);
  } catch (error: any) {
    console.error('❌ Erro ao obter status da fila:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter status da fila' },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const action = searchParams.get('action');
    const idSistema = searchParams.get('idSistema');
    const empresa = searchParams.get('empresa');

    if (action === 'force-sync' && idSistema && empresa) {
      await syncQueueService.forceSyncForContract(parseInt(idSistema), empresa);
      return NextResponse.json({ 
        success: true, 
        message: `Sincronização agendada para ${empresa}` 
      });
    }

    return NextResponse.json(
      { error: 'Ação inválida ou parâmetros faltando' },
      { status: 400 }
    );
  } catch (error: any) {
    console.error('❌ Erro ao processar requisição:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao processar requisição' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
