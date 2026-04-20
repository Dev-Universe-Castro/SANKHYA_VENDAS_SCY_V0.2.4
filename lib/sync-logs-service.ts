
import { getOracleConnection } from './oracle-service';
import oracledb from 'oracledb';

export interface SyncLog {
  ID_LOG?: number;
  ID_SISTEMA: number;
  EMPRESA: string;
  TABELA: string;
  STATUS: 'SUCESSO' | 'FALHA';
  TOTAL_REGISTROS?: number;
  REGISTROS_INSERIDOS?: number;
  REGISTROS_ATUALIZADOS?: number;
  REGISTROS_DELETADOS?: number;
  DURACAO_MS?: number;
  MENSAGEM_ERRO?: string;
  DATA_INICIO: Date;
  DATA_FIM?: Date;
  DATA_CRIACAO?: Date;
}

export interface SyncLogFilter {
  idSistema?: number;
  tabela?: string;
  status?: 'SUCESSO' | 'FALHA';
  dataInicio?: Date;
  dataFim?: Date;
}

/**
 * Salvar log de sincronização
 */
export async function salvarLogSincronizacao(log: SyncLog): Promise<number> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `INSERT INTO AS_SYNC_LOGS (
        ID_SISTEMA,
        EMPRESA,
        TABELA,
        STATUS,
        TOTAL_REGISTROS,
        REGISTROS_INSERIDOS,
        REGISTROS_ATUALIZADOS,
        REGISTROS_DELETADOS,
        DURACAO_MS,
        MENSAGEM_ERRO,
        DATA_INICIO,
        DATA_FIM
      ) VALUES (
        :idSistema,
        :empresa,
        :tabela,
        :status,
        :totalRegistros,
        :registrosInseridos,
        :registrosAtualizados,
        :registrosDeletados,
        :duracaoMs,
        :mensagemErro,
        :dataInicio,
        :dataFim
      ) RETURNING ID_LOG INTO :idLog`,
      {
        idSistema: log.ID_SISTEMA,
        empresa: log.EMPRESA,
        tabela: log.TABELA,
        status: log.STATUS,
        totalRegistros: log.TOTAL_REGISTROS || 0,
        registrosInseridos: log.REGISTROS_INSERIDOS || 0,
        registrosAtualizados: log.REGISTROS_ATUALIZADOS || 0,
        registrosDeletados: log.REGISTROS_DELETADOS || 0,
        duracaoMs: log.DURACAO_MS || null,
        mensagemErro: log.MENSAGEM_ERRO || null,
        dataInicio: log.DATA_INICIO,
        dataFim: log.DATA_FIM || null,
        idLog: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      },
      { autoCommit: true }
    );

    const idLog = (result.outBinds as any).idLog[0];
    return idLog;
  } finally {
    await connection.close();
  }
}

/**
 * Buscar logs de sincronização com filtros
 */
export async function buscarLogsSincronizacao(
  filter: SyncLogFilter = {},
  limit: number = 100,
  offset: number = 0
): Promise<{ logs: SyncLog[]; total: number }> {
  const connection = await getOracleConnection();

  try {
    const conditions: string[] = [];
    const binds: any = {};

    if (filter.idSistema) {
      conditions.push('ID_SISTEMA = :idSistema');
      binds.idSistema = filter.idSistema;
    }

    if (filter.tabela) {
      conditions.push('TABELA = :tabela');
      binds.tabela = filter.tabela;
    }

    if (filter.status) {
      conditions.push('STATUS = :status');
      binds.status = filter.status;
    }

    if (filter.dataInicio) {
      conditions.push('DATA_CRIACAO >= :dataInicio');
      binds.dataInicio = filter.dataInicio;
    }

    if (filter.dataFim) {
      conditions.push('DATA_CRIACAO <= :dataFim');
      binds.dataFim = filter.dataFim;
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Buscar total de registros
    const countResult = await connection.execute(
      `SELECT COUNT(*) as TOTAL FROM AS_SYNC_LOGS ${whereClause}`,
      binds,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const total = (countResult.rows![0] as any).TOTAL;

    // Buscar logs com paginação - usar valores literais ao invés de bind parameters para evitar conflitos
    const result = await connection.execute(
      `SELECT * FROM (
        SELECT 
          ID_LOG,
          ID_SISTEMA,
          EMPRESA,
          TABELA,
          STATUS,
          TOTAL_REGISTROS,
          REGISTROS_INSERIDOS,
          REGISTROS_ATUALIZADOS,
          REGISTROS_DELETADOS,
          DURACAO_MS,
          MENSAGEM_ERRO,
          DATA_INICIO,
          DATA_FIM,
          DATA_CRIACAO,
          ROW_NUMBER() OVER (ORDER BY DATA_CRIACAO DESC) as RN
        FROM AS_SYNC_LOGS
        ${whereClause}
      )
      WHERE RN > ${offset} AND RN <= ${offset + limit}`,
      binds,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const logs = (result.rows as any[]).map(row => ({
      ID_LOG: row.ID_LOG,
      ID_SISTEMA: row.ID_SISTEMA,
      EMPRESA: row.EMPRESA,
      TABELA: row.TABELA,
      STATUS: row.STATUS,
      TOTAL_REGISTROS: row.TOTAL_REGISTROS,
      REGISTROS_INSERIDOS: row.REGISTROS_INSERIDOS,
      REGISTROS_ATUALIZADOS: row.REGISTROS_ATUALIZADOS,
      REGISTROS_DELETADOS: row.REGISTROS_DELETADOS,
      DURACAO_MS: row.DURACAO_MS,
      MENSAGEM_ERRO: row.MENSAGEM_ERRO,
      DATA_INICIO: row.DATA_INICIO,
      DATA_FIM: row.DATA_FIM,
      DATA_CRIACAO: row.DATA_CRIACAO
    }));

    return { logs, total };
  } finally {
    await connection.close();
  }
}

/**
 * Buscar estatísticas de logs
 */
export async function buscarEstatisticasLogs(filter: SyncLogFilter = {}): Promise<{
  totalSincronizacoes: number;
  sucessos: number;
  falhas: number;
  porTabela: { tabela: string; total: number; sucessos: number; falhas: number }[];
}> {
  const connection = await getOracleConnection();

  try {
    const conditions: string[] = [];
    const binds: any = {};

    if (filter.idSistema) {
      conditions.push('ID_SISTEMA = :idSistema');
      binds.idSistema = filter.idSistema;
    }

    if (filter.dataInicio) {
      conditions.push('DATA_CRIACAO >= :dataInicio');
      binds.dataInicio = filter.dataInicio;
    }

    if (filter.dataFim) {
      conditions.push('DATA_CRIACAO <= :dataFim');
      binds.dataFim = filter.dataFim;
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // Estatísticas gerais
    const statsResult = await connection.execute(
      `SELECT 
        COUNT(*) as TOTAL,
        SUM(CASE WHEN STATUS = 'SUCESSO' THEN 1 ELSE 0 END) as SUCESSOS,
        SUM(CASE WHEN STATUS = 'FALHA' THEN 1 ELSE 0 END) as FALHAS
      FROM AS_SYNC_LOGS ${whereClause}`,
      binds,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const stats = statsResult.rows![0] as any;

    // Estatísticas por tabela
    const porTabelaResult = await connection.execute(
      `SELECT 
        TABELA,
        COUNT(*) as TOTAL,
        SUM(CASE WHEN STATUS = 'SUCESSO' THEN 1 ELSE 0 END) as SUCESSOS,
        SUM(CASE WHEN STATUS = 'FALHA' THEN 1 ELSE 0 END) as FALHAS
      FROM AS_SYNC_LOGS ${whereClause}
      GROUP BY TABELA
      ORDER BY TOTAL DESC`,
      binds,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const porTabela = (porTabelaResult.rows as any[]).map(row => ({
      tabela: row.TABELA,
      total: row.TOTAL,
      sucessos: row.SUCESSOS,
      falhas: row.FALHAS
    }));

    return {
      totalSincronizacoes: stats.TOTAL,
      sucessos: stats.SUCESSOS,
      falhas: stats.FALHAS,
      porTabela
    };
  } finally {
    await connection.close();
  }
}

/**
 * Busca a data da última sincronização bem-sucedida para uma tabela e sistema
 */
export async function buscarDataUltimaSincronizacao(idSistema: number, tabela: string): Promise<Date | null> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `SELECT MAX(DATA_INICIO) as ULTIMA_DATA
       FROM AS_SYNC_LOGS
       WHERE ID_SISTEMA = :idSistema
         AND TABELA = :tabela
         AND STATUS = 'SUCESSO'`,
      { idSistema, tabela },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (result.rows && result.rows.length > 0) {
      const row = result.rows[0] as any;
      return row.ULTIMA_DATA || null;
    }

    return null;
  } catch (error) {
    console.error(`❌ Erro ao buscar última sincronização para ${tabela}:`, error);
    return null;
  } finally {
    await connection.close();
  }
}
