
import { NextResponse } from 'next/server';
import { buscarLogsSincronizacao, buscarEstatisticasLogs } from '@/lib/sync-logs-service';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    
    const action = searchParams.get('action');
    
    if (action === 'stats') {
      const idSistema = searchParams.get('idSistema');
      const dataInicio = searchParams.get('dataInicio');
      const dataFim = searchParams.get('dataFim');
      
      const filter: any = {};
      
      if (idSistema) filter.idSistema = parseInt(idSistema);
      if (dataInicio) filter.dataInicio = new Date(dataInicio);
      if (dataFim) filter.dataFim = new Date(dataFim);
      
      const stats = await buscarEstatisticasLogs(filter);
      return NextResponse.json(stats);
    }

    if (action === 'export') {
      const idSistema = searchParams.get('idSistema');
      const tabela = searchParams.get('tabela');
      const status = searchParams.get('status');
      const dataInicio = searchParams.get('dataInicio');
      const dataFim = searchParams.get('dataFim');

      const filter: any = {};
      
      if (idSistema) filter.idSistema = parseInt(idSistema);
      if (tabela) filter.tabela = tabela;
      if (status) filter.status = status;
      if (dataInicio) filter.dataInicio = new Date(dataInicio);
      if (dataFim) filter.dataFim = new Date(dataFim);

      // Buscar todos os logs sem paginação
      const resultado = await buscarLogsSincronizacao(filter, 10000, 0);
      
      // Criar CSV
      const csvHeader = 'Data,Empresa,Tabela,Status,Registros,Inseridos,Atualizados,Deletados,Duração (ms),Erro\n';
      const csvRows = resultado.logs.map(log => {
        const data = new Date(log.DATA_CRIACAO).toLocaleString('pt-BR');
        const erro = log.MENSAGEM_ERRO ? `"${log.MENSAGEM_ERRO.replace(/"/g, '""')}"` : '';
        return `"${data}","${log.EMPRESA}","${log.TABELA}","${log.STATUS}",${log.TOTAL_REGISTROS},${log.REGISTROS_INSERIDOS},${log.REGISTROS_ATUALIZADOS},${log.REGISTROS_DELETADOS},${log.DURACAO_MS || 0},${erro}`;
      }).join('\n');

      const csv = csvHeader + csvRows;

      return new NextResponse(csv, {
        headers: {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': `attachment; filename="logs-sincronizacao-${new Date().toISOString().split('T')[0]}.csv"`
        }
      });
    }
    
    // Buscar logs com filtros
    const idSistema = searchParams.get('idSistema');
    const tabela = searchParams.get('tabela');
    const status = searchParams.get('status');
    const dataInicio = searchParams.get('dataInicio');
    const dataFim = searchParams.get('dataFim');
    const limit = parseInt(searchParams.get('limit') || '100');
    const offset = parseInt(searchParams.get('offset') || '0');

    const filter: any = {};
    
    if (idSistema) filter.idSistema = parseInt(idSistema);
    if (tabela) filter.tabela = tabela;
    if (status) filter.status = status;
    if (dataInicio) filter.dataInicio = new Date(dataInicio);
    if (dataFim) filter.dataFim = new Date(dataFim);

    const resultado = await buscarLogsSincronizacao(filter, limit, offset);
    
    return NextResponse.json(resultado);
  } catch (error: any) {
    console.error('❌ Erro ao buscar logs:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao buscar logs de sincronização' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
