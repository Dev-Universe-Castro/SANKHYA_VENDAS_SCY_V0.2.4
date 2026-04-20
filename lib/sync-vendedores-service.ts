
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

interface Vendedor {
  CODVEND: number;
  APELIDO?: string;
  ATIVO?: string;
  ATUACOMPRADOR?: string;
  CODCARGAHOR?: number;
  CODCENCUSPAD?: number;
  CODEMP?: number;
  CODFORM?: number;
  CODFUNC?: number;
  CODGER?: number;
  CODPARC?: number;
  CODREG?: number;
  CODUSU?: number;
  COMCM?: string;
  COMGER?: number;
  COMVENDA?: number;
  DESCMAX?: number;
  DIACOM?: number;
  DTALTER?: string;
  EMAIL?: string;
  GRUPODESCVEND?: string;
  GRUPORETENCAO?: string;
  PARTICMETA?: number;
  PERCCUSVAR?: number;
  PROVACRESC?: number;
  PROVACRESCCAC?: number;
  RECHREXTRA?: string;
  SALDODISP?: number;
  SALDODISPCAC?: number;
  SENHA?: number;
  TIPCALC?: string;
  TIPFECHCOM?: string;
  TIPOCERTIF?: string;
  TIPVALOR?: string;
  TIPVEND?: string;
  VLRHORA?: number;
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
 * Buscar vendedores do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
async function buscarVendedoresSankhyaTotal(
  idSistema: number,
  bearerToken: string,
  retryCount: number = 0
): Promise<Vendedor[]> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  console.log(`📋 [Sync] Buscando vendedores do Sankhya para empresa ${idSistema}... (tentativa ${retryCount + 1}/${MAX_RETRIES})`);

  let allVendedores: Vendedor[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      console.log(`📄 [Sync] Buscando página ${currentPage} de vendedores...`);

      const payload = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "Vendedor",
            "includePresentationFields": "N",
            "useFileBasedPagination": true,
            "disableRowsLimit": true,
            "offsetPage": currentPage.toString(),
            "entity": {
              "fieldset": {
                "list": "CODVEND,APELIDO,ATIVO,ATUACOMPRADOR,CODCARGAHOR,CODCENCUSPAD,CODEMP,CODFORM,CODFUNC,CODGER,CODPARC,CODREG,CODUSU,COMCM,COMGER,COMVENDA,DESCMAX,DIACOM,DTALTER,EMAIL,GRUPODESCVEND,GRUPORETENCAO,PARTICMETA,PERCCUSVAR,PROVACRESC,PROVACRESCCAC,RECHREXTRA,SALDODISP,SALDODISPCAC,SENHA,TIPCALC,TIPFECHCOM,TIPOCERTIF,TIPVALOR,TIPVEND,VLRHORA"
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

      const axios = require('axios');
      const response = await axios.post(URL_CONSULTA_ATUAL, payload, {
        headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
        timeout: 60000
      });

      if (!response.data?.responseBody?.entities?.entity) break;

      const entities = response.data.responseBody.entities;
      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const vendedoresPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
        return cleanObject as Vendedor;
      });

      allVendedores = allVendedores.concat(vendedoresPagina);
      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (vendedoresPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    return allVendedores;
  } catch (error: any) {
    if (retryCount < MAX_RETRIES - 1) {
      if (error.response?.status === 401 || error.response?.status === 403) {
        currentToken = await obterToken(idSistema, true);
        return buscarVendedoresSankhyaTotal(idSistema, currentToken, retryCount + 1);
      }
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
      return buscarVendedoresSankhyaTotal(idSistema, currentToken, retryCount + 1);
    }
    throw error;
  }
}

/**
 * Buscar vendedores do Sankhya alterados após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarVendedoresSankhyaParcial(
  idSistema: number,
  bearerToken: string,
  dataUltimaSync: Date,
  retryCount: number = 0
): Promise<Vendedor[]> {
  const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
  const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
  const ano = dataUltimaSync.getFullYear();
  const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
  const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
  const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
  const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

  console.log(`🔍 [Sync] [Parcial] Buscando vendedores alterados desde ${dataFormatada}...`);

  let allVendedores: Vendedor[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      const payload = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "Vendedor",
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
                "list": "CODVEND,APELIDO,ATIVO,ATUACOMPRADOR,CODCARGAHOR,CODCENCUSPAD,CODEMP,CODFORM,CODFUNC,CODGER,CODPARC,CODREG,CODUSU,COMCM,COMGER,COMVENDA,DESCMAX,DIACOM,DTALTER,EMAIL,GRUPODESCVEND,GRUPORETENCAO,PARTICMETA,PERCCUSVAR,PROVACRESC,PROVACRESCCAC,RECHREXTRA,SALDODISP,SALDODISPCAC,SENHA,TIPCALC,TIPFECHCOM,TIPOCERTIF,TIPVALOR,TIPVEND,VLRHORA"
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

      const axios = require('axios');
      const response = await axios.post(URL_CONSULTA_ATUAL, payload, {
        headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
        timeout: 60000
      });

      if (!response.data?.responseBody?.entities?.entity) break;

      const entities = response.data.responseBody.entities;
      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const vendedoresPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
        return cleanObject as Vendedor;
      });

      allVendedores = allVendedores.concat(vendedoresPagina);
      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (vendedoresPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    return allVendedores;
  } catch (error: any) {
    if (retryCount < 3 && (error.response?.status === 401 || error.response?.status === 403)) {
      currentToken = await obterToken(idSistema, true);
      return buscarVendedoresSankhyaParcial(idSistema, currentToken, dataUltimaSync, retryCount + 1);
    }
    throw error;
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_VENDEDORES 
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
    const partes = dataStr.trim().split(' ');
    const dataParte = partes[0];
    const horaParte = partes[1] || '00:00:00';
    const [dia, mes, ano] = dataParte.split('/');
    if (!dia || !mes || !ano) return null;
    const [hora, minuto, segundo] = horaParte.split(':');
    const date = new Date(parseInt(ano), parseInt(mes) - 1, parseInt(dia), parseInt(hora || '0'), parseInt(minuto || '0'), parseInt(segundo || '0'));
    return isNaN(date.getTime()) ? null : date;
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
  const maxValue = Math.pow(10, maxDigits - 2) - 0.01;
  return Math.abs(valorNum) > maxValue ? (valorNum > 0 ? maxValue : -maxValue) : valorNum;
}

/**
 * Upsert (inserir ou atualizar) vendedores
 */
async function upsertVendedores(
  connection: oracledb.Connection,
  idSistema: number,
  vendedores: Vendedor[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;
  const BATCH_SIZE = 100;

  for (let i = 0; i < vendedores.length; i += BATCH_SIZE) {
    const batch = vendedores.slice(i, i + BATCH_SIZE);
    for (const vendedor of batch) {
      try {
        const dtalter = parseDataSankhya(vendedor.DTALTER);
        const result = await connection.execute(
          `MERGE INTO AS_VENDEDORES dest
           USING (SELECT :idSistema AS ID_SISTEMA, :codvend AS CODVEND FROM DUAL) src
           ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODVEND = src.CODVEND)
           WHEN MATCHED THEN
             UPDATE SET
               APELIDO = :apelido, ATIVO = :ativo, ATUACOMPRADOR = :atuacomprador, 
               CODCARGAHOR = :codcargahor, CODCENCUSPAD = :codcencuspad, CODEMP = :codemp,
               CODFORM = :codform, CODFUNC = :codfunc, CODGER = :codger, CODPARC = :codparc,
               CODREG = :codreg, CODUSU = :codusu, COMCM = :comcm, COMGER = :comger,
               COMVENDA = :comvenda, DESCMAX = :descmax, DIACOM = :diacom, DTALTER = :dtalter,
               EMAIL = :email, GRUPODESCVEND = :grupodescvend, GRUPORETENCAO = :gruporetencao,
               PARTICMETA = :particmeta, PERCCUSVAR = :perccusvar, PROVACRESC = :provacresc,
               PROVACRESCCAC = :provacresccac, RECHREXTRA = :rechrextra, SALDODISP = :saldodisp,
               SALDODISPCAC = :saldodispcac, SENHA = :senha, TIPCALC = :tipcalc,
               TIPFECHCOM = :tipfechcom, TIPOCERTIF = :tipocertif, TIPVALOR = :tipvalor,
               TIPVEND = :tipvend, VLRHORA = :vlrhora, SANKHYA_ATUAL = 'S', DT_ULT_CARGA = CURRENT_TIMESTAMP
           WHEN NOT MATCHED THEN
             INSERT (
               ID_SISTEMA, CODVEND, APELIDO, ATIVO, ATUACOMPRADOR, CODCARGAHOR, CODCENCUSPAD, 
               CODEMP, CODFORM, CODFUNC, CODGER, CODPARC, CODREG, CODUSU, COMCM, COMGER, 
               COMVENDA, DESCMAX, DIACOM, DTALTER, EMAIL, GRUPODESCVEND, GRUPORETENCAO, 
               PARTICMETA, PERCCUSVAR, PROVACRESC, PROVACRESCCAC, RECHREXTRA, SALDODISP, 
               SALDODISPCAC, SENHA, TIPCALC, TIPFECHCOM, TIPOCERTIF, TIPVALOR, TIPVEND, 
               VLRHORA, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
             ) VALUES (
               :idSistema, :codvend, :apelido, :ativo, :atuacomprador, :codcargahor, :codcencuspad, 
               :codemp, :codform, :codfunc, :codger, :codparc, :codreg, :codusu, :comcm, :comger, 
               :comvenda, :descmax, :diacom, :dtalter, :email, :grupodescvend, :gruporetencao, 
               :particmeta, :perccusvar, :provacresc, :provacresccac, :rechrextra, :saldodisp, 
               :saldodispcac, :senha, :tipcalc, :tipfechcom, :tipocertif, :tipvalor, :tipvend, 
               :vlrhora, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
             )`,
          {
            idSistema, codvend: vendedor.CODVEND, apelido: vendedor.APELIDO || null, ativo: vendedor.ATIVO || null,
            atuacomprador: vendedor.ATUACOMPRADOR || null, codcargahor: vendedor.CODCARGAHOR || null,
            codcencuspad: vendedor.CODCENCUSPAD || null, codemp: vendedor.CODEMP || null, codform: vendedor.CODFORM || null,
            codfunc: vendedor.CODFUNC || null, codger: vendedor.CODGER || null, codparc: vendedor.CODPARC || null,
            codreg: vendedor.CODREG || null, codusu: vendedor.CODUSU || null, comcm: vendedor.COMCM || null,
            comger: validarValorNumerico(vendedor.COMGER), comvenda: validarValorNumerico(vendedor.COMVENDA),
            descmax: validarValorNumerico(vendedor.DESCMAX), diacom: vendedor.DIACOM || null, dtalter, email: vendedor.EMAIL || null,
            grupodescvend: vendedor.GRUPODESCVEND || null, gruporetencao: vendedor.GRUPORETENCAO || null,
            particmeta: validarValorNumerico(vendedor.PARTICMETA), perccusvar: validarValorNumerico(vendedor.PERCCUSVAR),
            provacresc: validarValorNumerico(vendedor.PROVACRESC), provacresccac: validarValorNumerico(vendedor.PROVACRESCCAC),
            rechrextra: vendedor.RECHREXTRA || null, saldodisp: validarValorNumerico(vendedor.SALDODISP),
            saldodispcac: validarValorNumerico(vendedor.SALDODISPCAC), senha: vendedor.SENHA || null,
            tipcalc: vendedor.TIPCALC || null, tipfechcom: vendedor.TIPFECHCOM || null, tipocertif: vendedor.TIPOCERTIF || null,
            tipvalor: vendedor.TIPVALOR || null, tipvend: vendedor.TIPVEND || null, vlrhora: validarValorNumerico(vendedor.VLRHORA)
          },
          { autoCommit: false }
        );

        if (result.rowsAffected && result.rowsAffected > 0) {
          // Estimativa simples para contagem
          inseridos++; // Simplificando a contagem de inseridos/atualizados para reduzir requisições
        }
      } catch (error: any) {
        console.error(`❌ [Sync] Erro ao processar vendedor CODVEND ${vendedor.CODVEND}:`, error.message);
      }
    }
    await connection.commit();
  }
  return { inseridos, atualizados: 0 }; // Simplificado
}

/**
 * Sincronização Total de vendedores
 */
export async function sincronizarVendedoresTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀 SINCRONIZAÇÃO TOTAL DE VENDEDORES: ${empresaNome}`);
    const bearerToken = await obterToken(idSistema, true);
    const vendedores = await buscarVendedoresSankhyaTotal(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertVendedores(connection, idSistema, vendedores);

    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_VENDEDORES', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: vendedores.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });

    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: vendedores.length,
      registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_VENDEDORES', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return {
      success: false, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message
    };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

/**
 * Sincronização Parcial de vendedores
 */
export async function sincronizarVendedoresParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀 SINCRONIZAÇÃO PARCIAL DE VENDEDORES: ${empresaNome}`);
    const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_VENDEDORES');
    if (!dataUltimaSync) return sincronizarVendedoresTotal(idSistema, empresaNome);

    const bearerToken = await obterToken(idSistema, true);
    const vendedores = await buscarVendedoresSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

    if (vendedores.length === 0) {
      return {
        success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
        registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
        dataInicio: dataInicio.toISOString(), dataFim: new Date().toISOString(), duracao: 0
      };
    }

    connection = await getOracleConnection();
    const { inseridos } = await upsertVendedores(connection, idSistema, vendedores);
    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_VENDEDORES', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: vendedores.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: 0,
      REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });

    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: vendedores.length,
      registrosInseridos: inseridos, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao
    };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_VENDEDORES', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return {
      success: false, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(), dataFim: dataFim.toISOString(), duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message
    };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

/**
 * Sincronizar vendedores de todas as empresas ativas
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  const connection = await getOracleConnection();
  const resultados: SyncResult[] = [];
  try {
    const result = await connection.execute(`SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`);
    await connection.close();
    const empresas = result.rows as any[];
    for (const empresa of empresas) {
      resultados.push(await sincronizarVendedoresTotal(empresa.ID_EMPRESA, empresa.EMPRESA));
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    return resultados;
  } catch (error: any) {
    console.error('❌ Erro ao sincronizar todas as empresas:', error);
    throw error;
  }
}

export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
  const connection = await getOracleConnection();
  try {
    const query = idSistema ? `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_VENDEDORES WHERE ID_SISTEMA = :idSistema GROUP BY ID_SISTEMA` : `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_VENDEDORES GROUP BY ID_SISTEMA`;
    const result = await connection.execute(query, idSistema ? [idSistema] : [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return result.rows as any[];
  } finally {
    await connection.close();
  }
}

export async function listarVendedores(idSistema?: number) {
  const connection = await getOracleConnection();
  try {
    const whereClause = idSistema ? `WHERE V.ID_SISTEMA = :idSistema` : '';
    const result = await connection.execute(`SELECT V.ID_SISTEMA, C.EMPRESA as NOME_CONTRATO, V.CODVEND, V.APELIDO, V.ATIVO, V.TIPVEND, V.EMAIL, V.SANKHYA_ATUAL, V.DT_ULT_CARGA FROM AS_VENDEDORES V JOIN AD_CONTRATOS C ON C.ID_EMPRESA = V.ID_SISTEMA ${whereClause} ORDER BY V.ID_SISTEMA, V.APELIDO FETCH FIRST 500 ROWS ONLY`, idSistema ? { idSistema } : {}, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    return result.rows || [];
  } finally {
    await connection.close();
  }
}