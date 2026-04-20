
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

interface Financeiro {
  NUFIN: number;
  CODPARC?: number;
  CODEMP?: number;
  VLRDESDOB?: number;
  DTVENC?: string;
  DTNEG?: string;
  PROVISAO?: string;
  DHBAIXA?: string;
  VLRBAIXA?: number;
  RECDESP?: number;
  NOSSONUM?: string;
  CODCTABCOINT?: number;
  HISTORICO?: string;
  NUMNOTA?: number;
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
  dataInicio: string;
  dataFim: string;
  duracao: number;
  erro?: string;
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

/**
 * Buscar títulos financeiros do Sankhya com retry
 */
async function buscarFinanceiroSankhya(
  idSistema: number,
  bearerToken: string,
  retryCount: number = 0
): Promise<Financeiro[]> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  console.log(`📋 [Sync] Buscando títulos financeiros do Sankhya para empresa ${idSistema}... (tentativa ${retryCount + 1}/${MAX_RETRIES})`);

  let allFinanceiros: Financeiro[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      console.log(`📄 [Sync] Buscando página ${currentPage} de títulos financeiros...`);

      const payload = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "Financeiro",
            "includePresentationFields": "N",
            "useFileBasedPagination": true,
            "disableRowsLimit": true,
            "offsetPage": currentPage.toString(),
            "entity": {
              "fieldset": {
                "list": "NUFIN, CODPARC, CODEMP, VLRDESDOB, DTVENC, DTNEG, PROVISAO, DHBAIXA, VLRBAIXA, RECDESP, NOSSONUM, CODCTABCOINT, HISTORICO, NUMNOTA, DTALTER"
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

      try {
        const response = await axios.post(URL_CONSULTA_ATUAL, payload, {
          headers: {
            'Authorization': `Bearer ${currentToken}`,
            'Content-Type': 'application/json'
          },
          timeout: 60000
        });

        if (!response.data?.responseBody?.entities?.entity) {
          console.log(`⚠️ [Sync] Nenhum título financeiro encontrado na página ${currentPage}`);
          break;
        }

        const entities = response.data.responseBody.entities;
        const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
        const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

        const financeirosPagina = entityArray.map((rawEntity: any) => {
          const cleanObject: any = {};
          for (let i = 0; i < fieldNames.length; i++) {
            const fieldKey = `f${i}`;
            const fieldName = fieldNames[i];
            if (rawEntity[fieldKey]) {
              cleanObject[fieldName] = rawEntity[fieldKey].$;
            }
          }
          return cleanObject as Financeiro;
        });

        allFinanceiros = allFinanceiros.concat(financeirosPagina);
        console.log(`✅ [Sync] Página ${currentPage}: ${financeirosPagina.length} registros (total acumulado: ${allFinanceiros.length})`);

        const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

        if (financeirosPagina.length === 0 || !hasMoreResult) {
          hasMoreData = false;
          console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${financeirosPagina.length})`);
        } else {
          currentPage++;
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      } catch (pageError: any) {
        if (pageError.response?.status === 401 || pageError.response?.status === 403) {
          console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
          console.log(`📊 [Sync] Progresso mantido: ${allFinanceiros.length} registros acumulados`);
          currentToken = await obterToken(idSistema, true);
          console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
          await new Promise(resolve => setTimeout(resolve, 1000));
        } else {
          throw pageError;
        }
      }
    }

    console.log(`✅ [Sync] Total de ${allFinanceiros.length} títulos financeiros recuperados em ${currentPage} páginas`);
    return allFinanceiros;

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao buscar títulos financeiros (tentativa ${retryCount + 1}/${MAX_RETRIES}):`, error.message);

    if (retryCount < MAX_RETRIES - 1) {
      if (
        error.code === 'ECONNABORTED' ||
        error.code === 'ETIMEDOUT' ||
        error.code === 'ENOTFOUND' ||
        error.message?.includes('timeout') ||
        error.response?.status >= 500
      ) {
        console.log(`🔄 [Sync] Aguardando ${RETRY_DELAY}ms antes da próxima tentativa...`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));

        if (error.response?.status === 401 || error.response?.status === 403) {
          console.log(`🔄 [Sync] Token expirado, renovando...`);
          const novoToken = await obterToken(idSistema, true);
          return buscarFinanceiroSankhya(idSistema, novoToken, retryCount + 1);
        }

        return buscarFinanceiroSankhya(idSistema, bearerToken, retryCount + 1);
      }
    }

    throw new Error(`Erro ao buscar títulos financeiros após ${retryCount + 1} tentativas: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_FINANCEIRO 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
    [idSistema],
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Converter data do formato Sankhya para Date do Oracle
 */
function parseDataSankhya(dataStr: string | undefined): Date | null {
  if (!dataStr) return null;

  try {
    // Formato esperado: DD/MM/YYYY ou DD/MM/YYYY HH:MI:SS
    const partes = dataStr.trim().split(' ');
    const dataParte = partes[0];
    const horaParte = partes[1] || '00:00:00';

    const [dia, mes, ano] = dataParte.split('/');

    if (!dia || !mes || !ano) {
      return null;
    }

    const [hora, minuto, segundo] = horaParte.split(':');

    const date = new Date(
      parseInt(ano),
      parseInt(mes) - 1,
      parseInt(dia),
      parseInt(hora || '0'),
      parseInt(minuto || '0'),
      parseInt(segundo || '0')
    );

    if (isNaN(date.getTime())) {
      return null;
    }

    return date;
  } catch (error) {
    return null;
  }
}

/**
 * Validar e limitar valor numérico
 */
function validarValorNumerico(valor: number | undefined, maxDigits: number = 15): number | null {
  if (valor === undefined || valor === null) return null;

  const valorNum = Number(valor);
  if (isNaN(valorNum)) return null;

  // Limitar a 15 dígitos totais (para NUMBER(15,2))
  const maxValue = Math.pow(10, maxDigits - 2) - 0.01;
  if (Math.abs(valorNum) > maxValue) {
    console.warn(`⚠️ [Sync] Valor ${valorNum} excede precisão máxima, será limitado a ${maxValue}`);
    return valorNum > 0 ? maxValue : -maxValue;
  }

  return valorNum;
}

/**
 * Upsert (inserir ou atualizar) títulos financeiros
 */
async function upsertFinanceiro(
  connection: oracledb.Connection,
  idSistema: number,
  financeiros: Financeiro[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < financeiros.length; i += BATCH_SIZE) {
    const batch = financeiros.slice(i, i + BATCH_SIZE);

    for (const financeiro of batch) {
      try {
        // Converter datas
        const dtvenc = parseDataSankhya(financeiro.DTVENC);
        const dtneg = parseDataSankhya(financeiro.DTNEG);
        const dhbaixa = parseDataSankhya(financeiro.DHBAIXA);

        // Validar valores numéricos
        const vlrdesdob = validarValorNumerico(financeiro.VLRDESDOB);
        const vlrbaixa = validarValorNumerico(financeiro.VLRBAIXA);

        const checkResult = await connection.execute(
          `SELECT COUNT(*) as count FROM AS_FINANCEIRO 
         WHERE ID_SISTEMA = :idSistema AND NUFIN = :nufin`,
          [idSistema, financeiro.NUFIN],
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        const exists = (checkResult.rows as any[])[0].COUNT > 0;

        if (exists) {
          await connection.execute(
            `UPDATE AS_FINANCEIRO SET
            CODPARC = :codparc,
            CODEMP = :codemp,
            VLRDESDOB = :vlrdesdob,
            DTVENC = :dtvenc,
            DTNEG = :dtneg,
            PROVISAO = :provisao,
            DHBAIXA = :dhbaixa,
            VLRBAIXA = :vlrbaixa,
            RECDESP = :recdesp,
            NOSSONUM = :nossonum,
            CODCTABCOINT = :codctabcoint,
             HISTORICO = :historico,
             NUMNOTA = :numnota,
             DTALTER = :dtAlter,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
           WHERE ID_SISTEMA = :idSistema AND NUFIN = :nufin`,
            {
              codparc: financeiro.CODPARC || null,
              codemp: financeiro.CODEMP || null,
              vlrdesdob,
              dtvenc,
              dtneg,
              provisao: financeiro.PROVISAO || null,
              dhbaixa,
              vlrbaixa,
              recdesp: financeiro.RECDESP || null,
              nossonum: financeiro.NOSSONUM || null,
              codctabcoint: financeiro.CODCTABCOINT || null,
              historico: financeiro.HISTORICO || null,
              numnota: financeiro.NUMNOTA || null,
              dtAlter: parseDataSankhya(financeiro.DTALTER),
              idSistema,
              nufin: financeiro.NUFIN
            },
            { autoCommit: false }
          );
          atualizados++;
        } else {
          await connection.execute(
            `INSERT INTO AS_FINANCEIRO (
            ID_SISTEMA, NUFIN, CODPARC, CODEMP, VLRDESDOB, DTVENC, DTNEG,
            PROVISAO, DHBAIXA, VLRBAIXA, RECDESP, NOSSONUM, CODCTABCOINT,
            HISTORICO, NUMNOTA, DTALTER, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :nufin, :codparc, :codemp, :vlrdesdob, :dtvenc, :dtneg,
            :provisao, :dhbaixa, :vlrbaixa, :recdesp, :nossonum, :codctabcoint,
            :historico, :numnota, :dtAlter, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
            {
              idSistema,
              nufin: financeiro.NUFIN,
              codparc: financeiro.CODPARC || null,
              codemp: financeiro.CODEMP || null,
              vlrdesdob,
              dtvenc,
              dtneg,
              provisao: financeiro.PROVISAO || null,
              dhbaixa,
              vlrbaixa,
              recdesp: financeiro.RECDESP || null,
              nossonum: financeiro.NOSSONUM || null,
              codctabcoint: financeiro.CODCTABCOINT || null,
              historico: financeiro.HISTORICO || null,
              numnota: financeiro.NUMNOTA || null,
              dtAlter: parseDataSankhya(financeiro.DTALTER)
            },
            { autoCommit: false }
          );
          inseridos++;
        }
      } catch (error: any) {
        console.error(`❌ [Sync] Erro ao processar financeiro NUFIN ${financeiro.NUFIN}:`, error.message);
      }
    }

    await connection.commit();
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(financeiros.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar títulos financeiros de uma empresa específica
 */
export async function sincronizarFinanceiroPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE FINANCEIRO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const financeiros = await buscarFinanceiroSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertFinanceiro(connection, idSistema, financeiros);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${financeiros.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_FINANCEIRO',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: financeiros.length,
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
      totalRegistros: financeiros.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar financeiro para ${empresaNome}:`, error);

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
        TABELA: 'AS_FINANCEIRO',
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
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
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
 * Sincronizar financeiro de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de financeiro de todas as empresas...');

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
      const resultado = await sincronizarFinanceiroPorEmpresa(
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
export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
  let connection: oracledb.Connection | undefined;

  try {
    connection = await getOracleConnection();

    const query = idSistema
      ? `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_FINANCEIRO
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_FINANCEIRO
        GROUP BY ID_SISTEMA`;

    const result = await connection.execute(
      query,
      idSistema ? [idSistema] : [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows as any[];

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao obter estatísticas:', error);
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
 * Busca títulos financeiros alterados (SINCRONIZAÇÃO PARCIAL)
 */
export async function buscarFinanceiroSankhyaParcial(
  idSistema: number,
  bearerToken: string,
  dataUltimaSync: Date
): Promise<Financeiro[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}`;

  console.log(`🔍 [Sync] [Parcial] Buscando títulos financeiros alterados desde ${dataFormatada}`);

  let allFinanceiros: Financeiro[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    const payload = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Financeiro",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "criteria": { "expression": { "$": `DTALTER > TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI')` } },
          "entity": { "fieldset": { "list": "NUFIN, CODPARC, CODEMP, VLRDESDOB, DTVENC, DTNEG, PROVISAO, DHBAIXA, VLRBAIXA, RECDESP, NOSSONUM, CODCTABCOINT, HISTORICO, NUMNOTA, DTALTER" } }
        }
      }
    };

    const contrato = await buscarContrato(idSistema);
    if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
    const isSandbox = contrato.IS_SANDBOX === true;
    const baseUrl = isSandbox ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
    const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    try {
      const response = await axios.post(URL_CONSULTA_ATUAL, payload, {
        headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
        timeout: 60000
      });
      const entities = response.data.responseBody?.entities;
      if (!entities || !entities.entity) break;
      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];
      const financeirosPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
        }
        return cleanObject as Financeiro;
      });
      allFinanceiros = allFinanceiros.concat(financeirosPagina);
      if (financeirosPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
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
  return allFinanceiros;
}

/**
 * Sincronização parcial de títulos financeiros
 */
export async function sincronizarFinanceiroParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;
  try {
    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_FINANCEIRO');
    if (!dataUltimaSync) return sincronizarFinanceiroPorEmpresa(idSistema, empresaNome);

    const bearerToken = await obterToken(idSistema, true);
    const financeiros = await buscarFinanceiroSankhyaParcial(idSistema, bearerToken, dataUltimaSync);
    if (financeiros.length === 0) {
      return {
        success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
        registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
        dataInicio: dataInicio.toISOString(), dataFim: new Date().toISOString(), duracao: 0
      };
    }
    connection = await getOracleConnection();
    const { inseridos, atualizados } = await upsertFinanceiro(connection, idSistema, financeiros);
    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_FINANCEIRO', STATUS: 'SUCESSO',
        TOTAL_REGISTROS: financeiros.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
        REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
      });
    } catch { }
    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: financeiros.length,
      registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_FINANCEIRO', STATUS: 'FALHA',
        TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
        DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
        DATA_INICIO: dataInicio, DATA_FIM: dataFim
      });
    } catch { }
    return {
      success: false, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message
    };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}
