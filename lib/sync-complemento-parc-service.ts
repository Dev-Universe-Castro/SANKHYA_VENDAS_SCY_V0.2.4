import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import { buscarDataUltimaSincronizacao, salvarLogSincronizacao } from './sync-logs-service';

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

/**
 * Faz requisição autenticada usando um Bearer Token específico
 * Usado durante sincronização para garantir que usamos o Bearer Token correto
 */
async function fazerRequisicaoAutenticadaComBearer(url: string, bearerToken: string, data: any, retryCount: number = 0): Promise<any> {
  const axios = require('axios');
  const MAX_RETRIES = 3;

  const config = {
    method: 'POST',
    url: url,
    data: data,
    headers: {
      'Authorization': `Bearer ${bearerToken}`,
      'Content-Type': 'application/json'
    },
    timeout: 60000
  };

  try {
    const response = await axios(config);
    return response.data;
  } catch (error: any) {
    if (error.response?.status === 401 || error.response?.status === 403) {
      console.log('🔑 [Sync] Token expirado detectado (401/403) - propagando para renovação...');
      const authError: any = new Error('TOKEN_EXPIRED');
      authError.isAuthError = true;
      authError.originalError = error;
      throw authError;
    }

    if (retryCount < MAX_RETRIES &&
      (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.response?.status >= 500)) {
      console.log(`🔄 [Sync] Retry ${retryCount + 1}/${MAX_RETRIES} após erro de rede...`);
      await new Promise(resolve => setTimeout(resolve, 2000 * (retryCount + 1)));
      return fazerRequisicaoAutenticadaComBearer(url, bearerToken, data, retryCount + 1);
    }

    console.error('❌ [Sync] Erro na requisição:', error.response?.data || error.message);
    throw new Error(`Erro ao buscar dados: ${error.response?.data?.statusMessage || error.message}`);
  }
}

interface ComplementoParcSankhya {
  CODPARC: number;
  SUGTIPNEGSAID?: number;
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
 * Busca Complemento Parceiro do Sankhya alterados após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarComplementoParcSankhyaParcial(idSistema: number, bearerToken: string, dataUltimaSync: Date): Promise<ComplementoParcSankhya[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

  console.log(`🔍 [Sync] Buscando complemento parceiro alterados desde ${dataFormatada}`);

  let allComplementoParceiros: ComplementoParcSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "ComplementoParc",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "criteria": {
            "expression": {
              "$": `DTALTER >= TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI:SS')`
            }
          },
          "entity": {
            "fieldset": {
              "list": "CODPARC, SUGTIPNEGSAID"
            }
          }
        }
      }
    };

    const contrato = await buscarContrato(idSistema);
    if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
    const isSandbox = contrato.IS_SANDBOX === true;
    const baseUrl = isSandbox ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
    const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    try {
      const respostaCompleta = await fazerRequisicaoAutenticadaComBearer(URL_CONSULTA_ATUAL, currentToken, PAYLOAD);
      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const compPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as ComplementoParcSankhya;
      });

      allComplementoParceiros = allComplementoParceiros.concat(compPagina);
      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (compPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (pageError: any) {
      if (pageError.isAuthError) {
        currentToken = await obterToken(idSistema, true);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        throw pageError;
      }
    }
  }

  return allComplementoParceiros;
}

/**
 * Sincronização Parcial de Complemento Parceiro
 */
export async function sincronizarComplementoParcParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO PARCIAL DE COMPLEMENTO PARCEIRO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema} - ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_COMPLEMENTO_PARC');

    if (!dataUltimaSync) {
      console.log(`⚠️ [Sync] Nenhuma sincronização anterior encontrada. Executando Sincronização Total...`);
      return sincronizarComplementoParcTotal(idSistema, empresaNome);
    }

    const bearerToken = await obterToken(idSistema, true);
    const complementoParcs = await buscarComplementoParcSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

    if (complementoParcs.length === 0) {
      console.log(`✅ [Sync] Nenhum novo dado para sincronizar.`);
      const dataFim = new Date();
      return {
        success: true,
        idSistema,
        empresa: empresaNome,
        totalRegistros: 0,
        registrosInseridos: 0,
        registrosAtualizados: 0,
        registrosDeletados: 0,
        dataInicio,
        dataFim,
        duracao: dataFim.getTime() - dataInicio.getTime()
      };
    }

    connection = await getOracleConnection();
    const { inseridos, atualizados } = await upsertComplementoParc(connection, idSistema, complementoParcs);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_COMPLEMENTO_PARC',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: complementoParcs.length,
      REGISTROS_INSERIDOS: inseridos,
      REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: 0,
      DURACAO_MS: duracao,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    });

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: complementoParcs.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados: 0,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_COMPLEMENTO_PARC',
      STATUS: 'FALHA',
      TOTAL_REGISTROS: 0,
      REGISTROS_INSERIDOS: 0,
      REGISTROS_ATUALIZADOS: 0,
      REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(),
      MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
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
 * Busca todos os Complementos Parceiro do Sankhya para uma empresa específica (SINCRONIZAÇÃO TOTAL)
 */
async function buscarComplementoParcSankhyaTotal(idSistema: number, bearerToken: string): Promise<ComplementoParcSankhya[]> {
  console.log(`🔍 [Sync] Buscando complemento de parceiros usando Bearer Token: ${bearerToken.substring(0, 50)}...`);

  let allComplementos: ComplementoParcSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    console.log(`📄 [Sync] Buscando página ${currentPage} de complemento de parceiros...`);

    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "ComplementoParc",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "entity": {
            "fieldset": {
              "list": "CODPARC, SUGTIPNEGSAID"
            }
          }
        }
      }
    };

    const contrato = await buscarContrato(idSistema);
    if (!contrato) {
      throw new Error(`Contrato ${idSistema} não encontrado`);
    }
    const isSandbox = contrato.IS_SANDBOX === true;
    const baseUrl = isSandbox
      ? "https://api.sandbox.sankhya.com.br"
      : "https://api.sankhya.com.br";
    const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    try {
      const respostaCompleta = await fazerRequisicaoAutenticadaComBearer(
        URL_CONSULTA_ATUAL,
        currentToken,
        PAYLOAD
      );

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log(`⚠️ [Sync] Nenhum complemento parceiro encontrado na página ${currentPage}`);
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const compPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as ComplementoParcSankhya;
      });

      allComplementos = allComplementos.concat(compPagina);
      console.log(`✅ [Sync] Página ${currentPage}: ${compPagina.length} registros (total acumulado: ${allComplementos.length})`);

      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (compPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
        console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${compPagina.length})`);
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (pageError: any) {
      if (pageError.isAuthError) {
        console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
        try {
          currentToken = await obterToken(idSistema, true);
          await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (tokenError) {
          throw new Error(`Falha ao renovar token após expiração: ${tokenError}`);
        }
      } else {
        console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, pageError.message);
        throw pageError;
      }
    }
  }

  console.log(`✅ [Sync] Total de ${allComplementos.length} complementos de parceiros recuperados`);
  return allComplementos;
}

/**
 * Executa o soft delete (marca como não atual) todos os complementos parceiros do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_COMPLEMENTO_PARC 
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
 * Executa UPSERT de complemento parceiro usando MERGE
 */
async function upsertComplementoParc(
  connection: oracledb.Connection,
  idSistema: number,
  complementos: ComplementoParcSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < complementos.length; i += BATCH_SIZE) {
    const batch = complementos.slice(i, i + BATCH_SIZE);

    for (const comp of batch) {
      try {
        const result = await connection.execute(
          `MERGE INTO AS_COMPLEMENTO_PARC dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codParc AS CODPARC FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPARC = src.CODPARC)
         WHEN MATCHED THEN
           UPDATE SET
             SUGTIPNEGSAID = :sugTipNegSaid,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPARC, SUGTIPNEGSAID, SANKHYA_ATUAL,
              DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codParc, :sugTipNegSaid, 'S',
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
          {
            idSistema,
            codParc: comp.CODPARC,
            sugTipNegSaid: comp.SUGTIPNEGSAID || null
          },
          { autoCommit: false }
        );

        if (result.rowsAffected && result.rowsAffected > 0) {
          const checkResult = await connection.execute(
            `SELECT DT_CRIACAO FROM AS_COMPLEMENTO_PARC 
           WHERE ID_SISTEMA = :idSistema AND CODPARC = :codParc`,
            { idSistema, codParc: comp.CODPARC },
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
      } catch (error: any) {
        console.error(`❌ [Sync] Erro ao processar complemento parceiro ${comp.CODPARC}:`, error.message);
      }
    }

    console.log(`📦 [Sync] Lote ${Math.floor(i / BATCH_SIZE) + 1} processado, fazendo commit...`);
    await connection.commit();
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincronização Total de Complemento Parceiro
 */
export async function sincronizarComplementoParcTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE COMPLEMENTO PARCEIRO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    const bearerToken = await obterToken(idSistema, true);
    const complementos = await buscarComplementoParcSankhyaTotal(idSistema, bearerToken);

    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertComplementoParc(connection, idSistema, complementos);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    
    // Salvar log de sucesso
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_COMPLEMENTO_PARC',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: complementos.length,
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
      totalRegistros: complementos.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados: registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar complementos parceiro para ${empresaNome}:`, error);

    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {}
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_COMPLEMENTO_PARC',
        STATUS: 'FALHA',
        TOTAL_REGISTROS: 0,
        REGISTROS_INSERIDOS: 0,
        REGISTROS_ATUALIZADOS: 0,
        REGISTROS_DELETADOS: 0,
        DURACAO_MS: duracao,
        MENSAGEM_ERRO: error.message,
        DATA_INICIO: dataInicio,
        DATA_FIM: dataFim
      });
    } catch (logError) {}

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
      } catch (closeError) {}
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
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_COMPLEMENTO_PARC
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
