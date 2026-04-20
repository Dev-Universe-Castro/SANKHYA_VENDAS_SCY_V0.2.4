import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { fazerRequisicaoAutenticada, obterToken } from './sankhya-api';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

/**
 * Helper to parse Sankhya dates (DD/MM/YYYY HH:MI:SS) to JS Date
 */
function parseDataSankhya(dataStr: string | undefined): Date | null {
  if (!dataStr) return null;
  try {
    const partes = dataStr.trim().split(' ');
    const dataParte = partes[0];
    const horaParte = partes[1] || '00:00:00';
    const [dia, mes, ano] = dataParte.split('/');
    if (!dia || !mes || !ano) return isNaN(new Date(dataStr).getTime()) ? null : new Date(dataStr);
    const [hora, minuto, segundo] = horaParte.split(':');
    const date = new Date(parseInt(ano), parseInt(mes) - 1, parseInt(dia), parseInt(hora || '0'), parseInt(minuto || '0'), parseInt(segundo || '0'));
    return isNaN(date.getTime()) ? null : date;
  } catch { return null; }
}

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

/**
 * Função auxiliar para buscar contrato por ID
 */
async function buscarContrato(id: number) {
  try {
    const { buscarContratoPorId } = await import('./oracle-service');
    return await buscarContratoPorId(id);
  } catch (error) {
    console.error(`Erro ao buscar contrato ${id}:`, error);
    return null;
  }
}

interface TipoOperacaoSankhya {
  CODTIPOPER: number;
  DESCROPER: string;
  ATIVO: string;
  DHALTER?: string;
}

interface SyncResult {
  success: boolean;
  idSistema: number;
  empresa: string;
  totalRegistros: number;
  registrosInseridos: number;
  registrosAtualizados: number;
  registrosDeletados: number;
  dataInicio: Date;
  dataFim: Date;
  duracao: number;
  erro?: string;
}

/**
 * Busca todos os tipos de operação do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
async function buscarTiposOperacaoSankhyaTotal(idSistema: number, bearerToken: string): Promise<TipoOperacaoSankhya[]> {
  console.log(`🔍 [Sync] Buscando tipos de operação do Sankhya para ID_SISTEMA: ${idSistema}`);

  let allTipos: TipoOperacaoSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      console.log(`📄 [Sync] Buscando página ${currentPage} de tipos de operação...`);

      const PAYLOAD = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "TipoOperacao",
            "includePresentationFields": "N",
            "useFileBasedPagination": true,
            "disableRowsLimit": true,
            "offsetPage": currentPage.toString(),
            "entity": {
              "fieldset": {
                "list": "CODTIPOPER, DESCROPER, ATIVO, DHALTER"
              }
            },
            "criteria": {
              "expression": {
                "$": "ATIVO = 'S'"
              }
            },
            "orderBy": {
              "expression": {
                "$": "DESCROPER ASC"
              }
            }
          }
        }
      };

      // Reutilizar o bearerToken durante toda a paginação
      const contrato = await buscarContrato(idSistema);
      if (!contrato) {
        throw new Error(`Contrato ${idSistema} não encontrado`);
      }
      const isSandbox = contrato.IS_SANDBOX === true;
      const baseUrl = isSandbox
        ? "https://api.sandbox.sankhya.com.br"
        : "https://api.sankhya.com.br";
      const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

      const axios = require('axios');

      try {
        const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
          headers: {
            'Authorization': `Bearer ${currentToken}`,
            'Content-Type': 'application/json'
          },
          timeout: 60000
        });

        const respostaCompleta = response.data;

        const entities = respostaCompleta.responseBody?.entities;

        if (!entities || !entities.entity) {
          console.log(`⚠️ [Sync] Nenhum tipo de operação encontrado na página ${currentPage}`);
          break;
        }

        const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
        const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

        const tiposPagina = entityArray.map((rawEntity: any) => {
          const cleanObject: any = {};
          for (let i = 0; i < fieldNames.length; i++) {
            const fieldKey = `f${i}`;
            const fieldName = fieldNames[i];
            if (rawEntity[fieldKey]) {
              cleanObject[fieldName] = rawEntity[fieldKey].$;
            }
          }
          return cleanObject as TipoOperacaoSankhya;
        });

        allTipos = allTipos.concat(tiposPagina);
        console.log(`✅ [Sync] Página ${currentPage}: ${tiposPagina.length} registros (total acumulado: ${allTipos.length})`);

        const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

        if (tiposPagina.length === 0 || !hasMoreResult) {
          hasMoreData = false;
          console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${tiposPagina.length})`);
        } else {
          currentPage++;
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      } catch (error: any) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
          console.log(`📊 [Sync] Progresso mantido: ${allTipos.length} registros acumulados`);
          currentToken = await obterToken(idSistema, true);
          console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
          await new Promise(resolve => setTimeout(resolve, 1000));
        } else {
          console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
          throw error;
        }
      }
    }
  } catch (error) {
    console.error(`❌ [Sync] Erro ao buscar tipos de operação na página ${currentPage}:`, error);
    throw error;
  }

  console.log(`✅ [Sync] Total de ${allTipos.length} tipos de operação recuperados em ${currentPage} páginas`);
  return allTipos;
}

/**
 * Busca tipos de operação do Sankhya alterados após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarTiposOperacaoSankhyaParcial(idSistema: number, bearerToken: string, dataUltimaSync: Date): Promise<TipoOperacaoSankhya[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

  console.log(`🔍 [Sync] [Parcial] Buscando tipos de operação alterados desde ${dataFormatada}`);

  let allTipos: TipoOperacaoSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "TipoOperacao",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "criteria": {
            "expression": {
              "$": `DHALTER >= TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI:SS')`
            }
          },
          "entity": {
            "fieldset": {
              "list": "CODTIPOPER, DESCROPER, ATIVO, DHALTER"
            }
          }
        }
      }
    };

    const contrato = await buscarContrato(idSistema);
    if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
    const baseUrl = contrato.IS_SANDBOX ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
    const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    try {
      const axios = require('axios');
      const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
        headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
        timeout: 60000
      });

      const entities = response.data.responseBody?.entities;
      if (!entities || !entities.entity) break;

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const tiposPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
        return cleanObject as TipoOperacaoSankhya;
      });

      allTipos = allTipos.concat(tiposPagina);
      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (tiposPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error: any) {
      if (error.response?.status === 401 || error.response?.status === 403) {
        currentToken = await obterToken(idSistema, true);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        throw error;
      }
    }
  }
  return allTipos;
}

/**
 * Sincronização Parcial de tipos de operação
 */
export async function sincronizarTiposOperacaoParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀 [Sync] [Parcial] TIPOS DE OPERAÇÃO: ${empresaNome}`);
    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_TIPOS_OPERACAO');

    if (!dataUltimaSync) return sincronizarTiposOperacaoTotal(idSistema, empresaNome);

    const bearerToken = await obterToken(idSistema, true);
    const tiposOperacao = await buscarTiposOperacaoSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

    if (tiposOperacao.length === 0) {
      return {
        success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
        registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
        dataInicio, dataFim: new Date(), duracao: 0
      };
    }

    connection = await getOracleConnection();
    const { inseridos, atualizados } = await upsertTiposOperacao(connection, idSistema, tiposOperacao);
    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TIPOS_OPERACAO', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: tiposOperacao.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });

    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: tiposOperacao.length,
      registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
      dataInicio, dataFim, duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TIPOS_OPERACAO', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return {
      success: false, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message
    };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

/**
 * Executa o soft delete (marca como não atual) todos os tipos de operação do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_TIPOS_OPERACAO
     SET SANKHYA_ATUAL = 'N',
         DT_ULT_CARGA = CURRENT_TIMESTAMP
     WHERE ID_SISTEMA = :idSistema
       AND SANKHYA_ATUAL = 'S'`,
    { idSistema },
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Executa UPSERT de tipos de operação usando MERGE
 */
async function upsertTiposOperacao(
  connection: oracledb.Connection,
  idSistema: number,
  tiposOperacao: TipoOperacaoSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < tiposOperacao.length; i += BATCH_SIZE) {
    const batch = tiposOperacao.slice(i, i + BATCH_SIZE);

    for (const tipo of batch) {
      const result = await connection.execute(
        `MERGE INTO AS_TIPOS_OPERACAO dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codTipOper AS CODTIPOPER FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODTIPOPER = src.CODTIPOPER)
         WHEN MATCHED THEN
           UPDATE SET
             DESCROPER = :descrOper,
             ATIVO = :ativo,
             DHALTER = :dhAlter,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODTIPOPER, DESCROPER, ATIVO, DHALTER, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codTipOper, :descrOper, :ativo, :dhAlter, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
        {
          idSistema,
          codTipOper: tipo.CODTIPOPER || null,
          descrOper: tipo.DESCROPER || null,
          ativo: tipo.ATIVO || 'S',
          dhAlter: parseDataSankhya(tipo.DHALTER)
        },
        { autoCommit: false }
      );

      if (result.rowsAffected && result.rowsAffected > 0) {
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_TIPOS_OPERACAO
           WHERE ID_SISTEMA = :idSistema AND CODTIPOPER = :codTipOper`,
          { idSistema, codTipOper: tipo.CODTIPOPER },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        if (checkResult.rows && checkResult.rows.length > 0) {
          const row: any = checkResult.rows[0];
          const dtCriacao = new Date(row.DT_CRIACAO);
          const agora = new Date();
          const diferencaMs = agora.getTime() - dtCriacao.getTime();

          if (diferencaMs < 5000) {
            inseridos++;
          } else {
            atualizados++;
          }
        }
      }
    }

    await connection.commit();
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(tiposOperacao.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincronização Total de tipos de operação
 */
export async function sincronizarTiposOperacaoTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE TIPOS DE OPERAÇÃO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    // SEMPRE forçar renovação do token para garantir credenciais corretas
    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    let bearerToken = await obterToken(idSistema, true);
    const tiposOperacao = await buscarTiposOperacaoSankhyaTotal(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertTiposOperacao(connection, idSistema, tiposOperacao);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${tiposOperacao.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_TIPOS_OPERACAO',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: tiposOperacao.length,
      REGISTROS_INSERIDOS: inseridos,
      REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados,
      DURACAO_MS: duracao,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    });

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: tiposOperacao.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar tipos de operação para ${empresaNome}:`, error);

    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('❌ [Sync] Erro ao fazer rollback:', rollbackError);
      }
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    // Salvar log de falha
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_TIPOS_OPERACAO',
      STATUS: 'FALHA',
      TOTAL_REGISTROS: 0,
      REGISTROS_INSERIDOS: 0,
      REGISTROS_ATUALIZADOS: 0,
      REGISTROS_DELETADOS: 0,
      DURACAO_MS: duracao,
      MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    }).catch(() => { });

    return {
      success: false,
      idSistema,
      empresa: empresaNome,
      totalRegistros: 0,
      registrosInseridos: 0,
      registrosAtualizados: 0,
      registrosDeletados: 0,
      dataInicio,
      dataFim,
      duracao,
      erro: error.message
    };

  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Sincroniza tipos de operação de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de tipos de operação de todas as empresas...');

  let connection: oracledb.Connection | undefined;
  const resultados: SyncResult[] = [];

  try {
    connection = await getOracleConnection();

    const result = await connection.execute(
      `SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    await connection.close();
    connection = undefined;

    if (!result.rows || result.rows.length === 0) {
      console.log('⚠️ [Sync] Nenhuma empresa ativa encontrada');
      return [];
    }

    const empresas = result.rows as any[];
    console.log(`📋 [Sync] ${empresas.length} empresas ativas encontradas`);

    // Sincronizar sequencialmente (uma por vez)
    for (const empresa of empresas) {
      const resultado = await sincronizarTiposOperacaoTotal(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

      // Aguardar 2 segundos entre sincronizações
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    const sucessos = resultados.filter(r => r.success).length;
    const falhas = resultados.filter(r => !r.success).length;

    console.log(`🏁 [Sync] Sincronização de todas as empresas concluída`);
    console.log(`✅ Sucessos: ${sucessos}, ❌ Falhas: ${falhas}`);

    return resultados;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao sincronizar todas as empresas:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Obter estatísticas de sincronização
 */
export async function obterEstatisticasSincronizacao(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT
         ID_SISTEMA,
         COUNT(*) as TOTAL_REGISTROS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_TIPOS_OPERACAO
       ${whereClause}
       GROUP BY ID_SISTEMA
       ORDER BY ID_SISTEMA`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}

/**
 * Listar tipos de operação sincronizados
 */
export async function listarTiposOperacao(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE T.ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
                T.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                T.CODTIPOPER,
                T.DESCROPER,
                T.SANKHYA_ATUAL,
                T.DT_ULT_CARGA
            FROM AS_TIPOS_OPERACAO T
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = T.ID_SISTEMA
            ${whereClause}
            ORDER BY T.ID_SISTEMA, T.DESCROPER
            FETCH FIRST 500 ROWS ONLY`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}