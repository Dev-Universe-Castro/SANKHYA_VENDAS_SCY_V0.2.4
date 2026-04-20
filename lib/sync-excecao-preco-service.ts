
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';
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

interface ExcecaoPreco {
  CODPROD: number;
  VLRANT?: number;
  VARIACAO?: number;
  NUTAB: number;
  TIPO?: string;
  VLRVENDA?: number;
  CODLOCAL: number;
  CONTROLE?: string;
  DHALTREG?: string;
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
 * Busca todas as exceções de preço do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
export async function buscarExcecaoPrecoSankhyaTotal(
  idSistema: number,
  bearerToken: string,
  retryCount: number = 0
): Promise<ExcecaoPreco[]> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  console.log(`📋 [Sync] Buscando exceções de preço do Sankhya para empresa ${idSistema}... (tentativa ${retryCount + 1}/${MAX_RETRIES})`);

  let allExcecoes: ExcecaoPreco[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      const payload = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "Excecao",
            "includePresentationFields": "N",
            "useFileBasedPagination": true,
            "disableRowsLimit": true,
            "offsetPage": currentPage.toString(),
            "entity": {
              "fieldset": {
                "list": "CODPROD, VLRANT, VARIACAO, NUTAB, TIPO, VLRVENDA, CODLOCAL, CONTROLE, DHALTREG"
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
        const response = await axios.post(URL_CONSULTA_ATUAL, payload, {
          headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
          timeout: 60000
        });

        if (!response.data?.responseBody?.entities?.entity) break;

        const entities = response.data.responseBody.entities;
        const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
        const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

        const excecoesPagina = entityArray.map((rawEntity: any) => {
          const cleanObject: any = {};
          for (let i = 0; i < fieldNames.length; i++) {
            const fieldKey = `f${i}`;
            const fieldName = fieldNames[i];
            if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
          return cleanObject as ExcecaoPreco;
        });

        allExcecoes = allExcecoes.concat(excecoesPagina);
        const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

        if (excecoesPagina.length === 0 || !hasMoreResult) {
          hasMoreData = false;
        } else {
          currentPage++;
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      } catch (pageError: any) {
        if (pageError.response?.status === 401 || pageError.response?.status === 403) {
          currentToken = await obterToken(idSistema, true);
          await new Promise(resolve => setTimeout(resolve, 1000));
        } else {
          throw pageError;
        }
      }
    }
    return allExcecoes;
  } catch (error: any) {
    if (retryCount < MAX_RETRIES - 1) {
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
      const novoToken = await obterToken(idSistema, true);
      return buscarExcecaoPrecoSankhyaTotal(idSistema, novoToken, retryCount + 1);
    }
    throw error;
  }
}

/**
 * Busca exceções de preço do Sankhya alteradas (SINCRONIZAÇÃO PARCIAL)
 */
export async function buscarExcecaoPrecoSankhyaParcial(
  idSistema: number,
  bearerToken: string,
  dataUltimaSync: Date
): Promise<ExcecaoPreco[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

  console.log(`🔍 [Sync] [Parcial] Buscando exceções de preço alteradas desde ${dataFormatada}`);

  let allExcecoes: ExcecaoPreco[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    const payload = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Excecao",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "criteria": { "expression": { "$": `DHALTREG >= TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI:SS')` } },
          "entity": { "fieldset": { "list": "CODPROD, VLRANT, VARIACAO, NUTAB, TIPO, VLRVENDA, CODLOCAL, CONTROLE, DHALTREG" } }
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
      const excecoesPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
        }
        return cleanObject as ExcecaoPreco;
      });
      allExcecoes = allExcecoes.concat(excecoesPagina);
      if (excecoesPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
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
  return allExcecoes;
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_EXCECAO_PRECO SET SANKHYA_ATUAL = 'N', DT_ULT_CARGA = CURRENT_TIMESTAMP WHERE ID_SISTEMA = :idSistema AND SANKHYA_ATUAL = 'S'`,
    [idSistema], { autoCommit: false }
  );
  return result.rowsAffected || 0;
}

/**
 * Upsert (inserir ou atualizar) exceções de preço usando MERGE
 */
async function upsertExcecaoPreco(
  connection: oracledb.Connection,
  idSistema: number,
  excecoes: ExcecaoPreco[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;
  const BATCH_SIZE = 100;
  for (let i = 0; i < excecoes.length; i += BATCH_SIZE) {
    const batch = excecoes.slice(i, i + BATCH_SIZE);
    for (const excecao of batch) {
      const result = await connection.execute(
        `MERGE INTO AS_EXCECAO_PRECO dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codProd AS CODPROD, :nuTab AS NUTAB, :codLocal AS CODLOCAL FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPROD = src.CODPROD AND dest.NUTAB = src.NUTAB AND dest.CODLOCAL = src.CODLOCAL)
         WHEN MATCHED THEN
           UPDATE SET VLRANT = :vlrAnt, VARIACAO = :variacao, TIPO = :tipo, VLRVENDA = :vlrVenda, CONTROLE = :controle, DHALTREG = :dhAltReg, SANKHYA_ATUAL = 'S', DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (ID_SISTEMA, CODPROD, NUTAB, CODLOCAL, VLRANT, VARIACAO, TIPO, VLRVENDA, CONTROLE, DHALTREG, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO)
           VALUES (:idSistema, :codProd, :nuTab, :codLocal, :vlrAnt, :variacao, :tipo, :vlrVenda, :controle, :dhAltReg, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        {
          idSistema, codProd: excecao.CODPROD, nuTab: excecao.NUTAB, codLocal: excecao.CODLOCAL,
          vlrAnt: excecao.VLRANT || null, variacao: excecao.VARIACAO || null, tipo: excecao.TIPO || null,
          vlrVenda: excecao.VLRVENDA || null, controle: excecao.CONTROLE || null, dhAltReg: parseDataSankhya(excecao.DHALTREG)
        }, { autoCommit: false }
      );
      if (result.rowsAffected && result.rowsAffected > 0) {
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_EXCECAO_PRECO WHERE ID_SISTEMA = :idSistema AND CODPROD = :codProd AND NUTAB = :nuTab AND CODLOCAL = :codLocal`,
          { idSistema, codProd: excecao.CODPROD, nuTab: excecao.NUTAB, codLocal: excecao.CODLOCAL },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        if (checkResult.rows && checkResult.rows.length > 0) {
          const row: any = checkResult.rows[0];
          const dtCriacao = new Date(row.DT_CRIACAO);
          if (new Date().getTime() - dtCriacao.getTime() < 5000) inseridos++; else atualizados++;
        }
      }
    }
    await connection.commit();
  }
  return { inseridos, atualizados };
}

export async function sincronizarExcecaoPrecoTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;
  try {
    const bearerToken = await obterToken(idSistema, true);
    const excecoes = await buscarExcecaoPrecoSankhyaTotal(idSistema, bearerToken);
    connection = await getOracleConnection();
    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertExcecaoPreco(connection, idSistema, excecoes);
    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_EXCECAO_PRECO', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: excecoes.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });
    return { success: true, idSistema, empresa: empresaNome, totalRegistros: excecoes.length, registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados, dataInicio, dataFim, duracao };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_EXCECAO_PRECO', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return { success: false, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

export async function sincronizarExcecaoPrecoParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;
  try {
    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_EXCECAO_PRECO');
    if (!dataUltimaSync) return sincronizarExcecaoPrecoTotal(idSistema, empresaNome);
    const bearerToken = await obterToken(idSistema, true);
    const excecoes = await buscarExcecaoPrecoSankhyaParcial(idSistema, bearerToken, dataUltimaSync);
    if (excecoes.length === 0) return { success: true, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim: new Date(), duracao: 0 };
    connection = await getOracleConnection();
    const { inseridos, atualizados } = await upsertExcecaoPreco(connection, idSistema, excecoes);
    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_EXCECAO_PRECO', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: excecoes.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });
    return { success: true, idSistema, empresa: empresaNome, totalRegistros: excecoes.length, registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0, dataInicio, dataFim, duracao };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_EXCECAO_PRECO', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return { success: false, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

export async function sincronizarExcecaoPrecoPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  return sincronizarExcecaoPrecoTotal(idSistema, empresaNome);
}

export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  const connection = await getOracleConnection();
  const result = await connection.execute(`SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  await connection.close();
  const resultados: SyncResult[] = [];
  const empresas = result.rows as any[];
  for (const empresa of empresas) {
    const resultado = await sincronizarExcecaoPrecoTotal(empresa.ID_EMPRESA, empresa.EMPRESA);
    resultados.push(resultado);
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  return resultados;
}

export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
  const connection = await getOracleConnection();
  const query = idSistema
    ? `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_EXCECAO_PRECO WHERE ID_SISTEMA = :idSistema GROUP BY ID_SISTEMA`
    : `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_EXCECAO_PRECO GROUP BY ID_SISTEMA`;
  const result = await connection.execute(query, idSistema ? [idSistema] : [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  await connection.close();
  return result.rows as any[];
}

export async function listarExcecaoPreco(idSistema?: number) {
  const connection = await getOracleConnection();
  try {
    const whereClause = idSistema ? `WHERE E.ID_SISTEMA = :idSistema` : '';
    const result = await connection.execute(`SELECT E.ID_SISTEMA, C.EMPRESA as NOME_CONTRATO, E.CODPROD, E.NUTAB, E.CODLOCAL, E.VLRVENDA, E.SANKHYA_ATUAL, E.DT_ULT_CARGA FROM AS_EXCECAO_PRECO E JOIN AD_CONTRATOS C ON C.ID_EMPRESA = E.ID_SISTEMA ${whereClause} ORDER BY E.ID_SISTEMA, E.CODPROD FETCH FIRST 500 ROWS ONLY`, idSistema ? { idSistema } : {}, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return result.rows || [];
  } finally { await connection.close(); }
}
