import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { fazerRequisicaoAutenticada, obterToken } from './sankhya-api';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

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

interface EstoqueSankhya {
  ESTOQUE: number;
  CODPROD: number;
  ATIVO: string;
  CONTROLE: string;
  CODLOCAL: string;
  DTALTER?: string;
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
 * Busca todos os estoques do Sankhya para uma empresa específica
 */
async function buscarEstoquesSankhya(idSistema: number, bearerToken: string): Promise<EstoqueSankhya[]> {
  console.log(`🔍 [Sync] Buscando TODOS os estoques do Sankhya para ID_SISTEMA: ${idSistema}`);

  let allEstoques: EstoqueSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    console.log(`📄 [Sync] Buscando página ${currentPage} de estoques...`);

    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Estoque",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "entity": {
            "fieldset": {
              "list": "ESTOQUE, CODPROD, ATIVO, CONTROLE, CODLOCAL, DTALTER"
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
        timeout: 300000
      });

      const respostaCompleta = response.data;

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log(`⚠️ [Sync] Nenhum estoque encontrado na página ${currentPage}`);
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const estoquesPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as EstoqueSankhya;
      });

      allEstoques = allEstoques.concat(estoquesPagina);
      console.log(`✅ [Sync] Página ${currentPage}: ${estoquesPagina.length} registros (total acumulado: ${allEstoques.length})`);

      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (estoquesPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
        console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${estoquesPagina.length})`);
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error: any) {
      if (error.response?.status === 401 || error.response?.status === 403) {
        console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
        console.log(`📊 [Sync] Progresso mantido: ${allEstoques.length} registros acumulados`);
        currentToken = await obterToken(idSistema, true);
        console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
        throw error;
      }
    }
  }

  console.log(`✅ [Sync] Total de ${allEstoques.length} estoques recuperados em ${currentPage} páginas`);
  return allEstoques;
}

/**
 * Executa o soft delete (marca como não atual) todos os estoques do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_ESTOQUES 
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
 * Executa UPSERT de estoques usando MERGE
 */
async function upsertEstoques(
  connection: oracledb.Connection,
  idSistema: number,
  estoques: EstoqueSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < estoques.length; i += BATCH_SIZE) {
    const batch = estoques.slice(i, i + BATCH_SIZE);

    for (const estoque of batch) {
      // Validar e normalizar o campo CONTROLE
      // Aceita apenas 'E' (Estoque) ou 'L' (Lote), default para 'E'
      let controleValido = 'E';
      if (estoque.CONTROLE) {
        const controleUpper = estoque.CONTROLE.toUpperCase().trim();
        if (controleUpper === 'E' || controleUpper === 'L') {
          controleValido = controleUpper;
        }
      }

      const result = await connection.execute(
        `MERGE INTO AS_ESTOQUES dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codProd AS CODPROD, :codLocal AS CODLOCAL FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPROD = src.CODPROD AND dest.CODLOCAL = src.CODLOCAL)
         WHEN MATCHED THEN
           UPDATE SET
             ESTOQUE = :estoque,
             ATIVO = :ativo,
             CONTROLE = :controle,
             DTALTER = :dtAlter,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPROD, CODLOCAL, ESTOQUE, ATIVO, CONTROLE, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codProd, :codLocal, :estoque, :ativo, :controle, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
        {
          idSistema,
          codProd: estoque.CODPROD || null,
          codLocal: estoque.CODLOCAL || null,
          estoque: estoque.ESTOQUE || 0,
          ativo: estoque.ATIVO || 'S',
          controle: controleValido,
          dtAlter: (function () {
            if (!estoque.DTALTER) return null;
            try {
              const partes = estoque.DTALTER.trim().split(' ');
              const dataParte = partes[0];
              const horaParte = partes[1] || '00:00:00';
              const [dia, mes, ano] = dataParte.split('/');
              if (!dia || !mes || !ano) return null;
              const [hora, minuto, segundo] = horaParte.split(':');
              return new Date(parseInt(ano), parseInt(mes) - 1, parseInt(dia), parseInt(hora || '0'), parseInt(minuto || '0'), parseInt(segundo || '0'));
            } catch { return null; }
          })()
        },
        { autoCommit: false }
      );

      if (result.rowsAffected && result.rowsAffected > 0) {
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_ESTOQUES 
           WHERE ID_SISTEMA = :idSistema AND CODPROD = :codProd AND CODLOCAL = :codLocal`,
          { idSistema, codProd: estoque.CODPROD, codLocal: estoque.CODLOCAL },
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
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(estoques.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincroniza estoques de uma empresa específica
 */
export async function sincronizarEstoquesPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE ESTOQUES`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    // SEMPRE forçar renovação do token para garantir credenciais corretas
    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);

    const estoques = await buscarEstoquesSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertEstoques(connection, idSistema, estoques);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${estoques.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_ESTOQUES',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: estoques.length,
        REGISTROS_INSERIDOS: inseridos,
        REGISTROS_ATUALIZADOS: atualizados,
        REGISTROS_DELETADOS: registrosDeletados,
        DURACAO_MS: duracao,
        DATA_INICIO: dataInicio,
        DATA_FIM: dataFim
      });
    } catch (logError) {
      console.error('❌ [Sync] Erro ao salvar log:', logError);
    }

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: estoques.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar estoques para ${empresaNome}:`, error);

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
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_ESTOQUES',
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
    } catch (logError) {
      console.error('❌ [Sync] Erro ao salvar log:', logError);
    }

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
 * Sincroniza estoques de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de estoques de todas as empresas...');

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

    for (const empresa of empresas) {
      const resultado = await sincronizarEstoquesPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

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
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN ESTOQUE ELSE 0 END) as ESTOQUE_TOTAL,
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_ESTOQUES
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
 * Busca estoques do Sankhya alterados (SINCRONIZAÇÃO PARCIAL)
 */
export async function buscarEstoquesSankhyaParcial(
  idSistema: number,
  bearerToken: string,
  dataUltimaSync: Date
): Promise<EstoqueSankhya[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}`;

  console.log(`🔍 [Sync] [Parcial] Buscando estoques alterados desde ${dataFormatada}`);

  let allEstoques: EstoqueSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Estoque",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "criteria": { "expression": { "$": `DTALTER > TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI')` } },
          "entity": { "fieldset": { "list": "ESTOQUE, CODPROD, ATIVO, CONTROLE, CODLOCAL, DTALTER" } }
        }
      }
    };

    const contrato = await buscarContrato(idSistema);
    if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
    const baseUrl = contrato.IS_SANDBOX === true ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
    const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    const axios = require('axios');
    try {
      const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
        headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
        timeout: 300000
      });
      const entities = response.data.responseBody?.entities;
      if (!entities || !entities.entity) break;
      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];
      const estoquesPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
        }
        return cleanObject as EstoqueSankhya;
      });
      allEstoques = allEstoques.concat(estoquesPagina);
      if (estoquesPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error: any) {
      if (error.response?.status === 401 || error.response?.status === 403) {
        currentToken = await obterToken(idSistema, true);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else throw error;
    }
  }
  return allEstoques;
}

/**
 * Sincronização parcial de estoques
 */
export async function sincronizarEstoquesParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;
  try {
    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_ESTOQUES');
    if (!dataUltimaSync) return sincronizarEstoquesPorEmpresa(idSistema, empresaNome);

    const bearerToken = await obterToken(idSistema, true);
    const estoques = await buscarEstoquesSankhyaParcial(idSistema, bearerToken, dataUltimaSync);
    if (estoques.length === 0) {
      return {
        success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
        registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
        dataInicio, dataFim: new Date(), duracao: 0
      };
    }
    connection = await getOracleConnection();
    const { inseridos, atualizados } = await upsertEstoques(connection, idSistema, estoques);
    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_ESTOQUES', STATUS: 'SUCESSO',
        TOTAL_REGISTROS: estoques.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
        REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
      });
    } catch { }
    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: estoques.length,
      registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
      dataInicio, dataFim, duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_ESTOQUES', STATUS: 'FALHA',
        TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
        DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
        DATA_INICIO: dataInicio, DATA_FIM: dataFim
      });
    } catch { }
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
 * Função especial para retomar sincronização interrompida de estoques
 */
export async function sincronizarEstoquesRetomada(idSistema: number, empresaNome: string, startPage: number): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`🚀 [Retomada] Iniciando sincronização de ESTOQUES na página ${startPage} para ID: ${idSistema}`);

    const bearerToken = await obterToken(idSistema, true);
    let currentPage = startPage;
    let hasMoreData = true;
    let currentToken = bearerToken;

    connection = await getOracleConnection();

    while (hasMoreData) {
      try {
        console.log(`📄 [Retomada] Buscando página ${currentPage} de estoques...`);

        const PAYLOAD = {
          "requestBody": {
            "dataSet": {
              "rootEntity": "Estoque",
              "includePresentationFields": "N",
              "useFileBasedPagination": true,
              "disableRowsLimit": true,
              "offsetPage": currentPage.toString(),
              "entity": {
                "fieldset": { "list": "ESTOQUE, CODPROD, ATIVO, CONTROLE, CODLOCAL, DTALTER" }
              }
            }
          }
        };

        const contrato = await buscarContrato(idSistema);
        if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
        const baseUrl = contrato.IS_SANDBOX ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
        const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

        const axios = require('axios');
        const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
          headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
          timeout: 300000
        });

        const entities = response.data.responseBody?.entities;
        if (!entities || !entities.entity) {
          hasMoreData = false;
          break;
        }

        const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
        const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

        const estoquesPagina = entityArray.map((rawEntity: any) => {
          const cleanObject: any = {};
          for (let i = 0; i < fieldNames.length; i++) {
            const fieldKey = `f${i}`;
            const fieldName = fieldNames[i];
            if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
          return cleanObject as EstoqueSankhya;
        });

        const { inseridos, atualizados } = await upsertEstoques(connection, idSistema, estoquesPagina);

        console.log(`✅ [Retomada] Página ${currentPage}: ${estoquesPagina.length} registros | 📥 Inseridos: ${inseridos} | 🔄 Atualizados: ${atualizados}`);

        const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;
        if (estoquesPagina.length === 0 || !hasMoreResult) {
          hasMoreData = false;
        } else {
          currentPage++;
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      } catch (error: any) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          console.log(`🔄 [Retomada] Token expirado na página ${currentPage}, renovando...`);
          currentToken = await obterToken(idSistema, true);
          await new Promise(resolve => setTimeout(resolve, 1000));
        } else {
          throw error;
        }
      }
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_ESTOQUES', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: 0,
      REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });

    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio, dataFim, duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_ESTOQUES', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    throw error;
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

/**
 * Listar estoques sincronizados
 */
export async function listarEstoques(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE E.ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
                E.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                E.CODPROD,
                E.CODLOCAL,
                E.ESTOQUE,
                E.CONTROLE,
                E.SANKHYA_ATUAL,
                E.DT_ULT_CARGA
            FROM AS_ESTOQUES E
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = E.ID_SISTEMA
            ${whereClause}
            ORDER BY E.ID_SISTEMA, E.CODPROD
            FETCH FIRST 500 ROWS ONLY`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}