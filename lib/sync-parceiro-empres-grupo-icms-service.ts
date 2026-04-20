import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

/**
 * Interface para os dados da tabela ParceiroEmpresGrupoIcms do Sankhya
 */
interface ParceiroEmpresGrupoIcmsSankhya {
  CLASSIFICMS?: string;
  CODEMP: number;
  CODPARC: number;
  CODTAB: number;
  FORMULA?: string;
  GRUPOICMS: number;
  INDPRECOEMBUT?: number;
  RETEMISS?: string;
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
 * Busca todos os registros do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
export async function buscarParceiroEmpresGrupoIcmsSankhyaTotal(
  idSistema: number,
  bearerToken: string,
  retryCount: number = 0
): Promise<ParceiroEmpresGrupoIcmsSankhya[]> {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  console.log(`📋 [Sync] Buscando ParceiroEmpresGrupoIcms do Sankhya para empresa ${idSistema}... (tentativa ${retryCount + 1}/${MAX_RETRIES})`);

  let allRegistros: ParceiroEmpresGrupoIcmsSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  try {
    while (hasMoreData) {
      console.log(`📄 [Sync] Buscando página ${currentPage} de ParceiroEmpresGrupoIcms...`);
      const payload = {
        "requestBody": {
          "dataSet": {
            "rootEntity": "ParceiroEmpresGrupoIcms",
            "includePresentationFields": "N",
            "useFileBasedPagination": true,
            "disableRowsLimit": true,
            "offsetPage": currentPage.toString(),
            "entity": {
              "fieldset": {
                "list": "CLASSIFICMS, CODEMP, CODPARC, CODTAB, FORMULA, GRUPOICMS, INDPRECOEMBUT, RETEMISS"
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

        const registrosPagina = entityArray.map((rawEntity: any) => {
          const cleanObject: any = {};
          for (let i = 0; i < fieldNames.length; i++) {
            const fieldKey = `f${i}`;
            const fieldName = fieldNames[i];
            if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
          
          // Mapeamento exaustivo para lidar com a "criatividade" dos campos no Sankhya
          const findVal = (possibleNames: string[]) => {
            const match = possibleNames.find(name => cleanObject[name] !== undefined && cleanObject[name] !== null);
            return match ? cleanObject[match] : null;
          };

          return {
            CLASSIFICMS: findVal(['CLASSIFICMS', 'CLASSICMS', 'CLASSIICMS']),
            CODEMP: findVal(['CODEMP', 'CODDEMP', 'CODEMPP']),
            CODPARC: findVal(['CODPARC', 'CODDPARC', 'CODPARCC']),
            CODTAB: findVal(['CODTAB', 'CODTTAB', 'CODDTAB', 'CODTABB', 'COODTAB', 'COODTTAB']),
            FORMULA: findVal(['FORMULA', 'FORRMULA', 'FORMULLA']),
            GRUPOICMS: findVal(['GRUPOICMS', 'GRUPPOICMS', 'GRUUPOICMS', 'GRUPPOOICMS']),
            INDPRECOEMBUT: findVal(['INDPRECOEMBUT', 'INDPRECOEMBUUT', 'INDPRECOEMBUTT']),
            RETEMISS: findVal(['RETEMISS', 'RETTEMISS', 'RETEEMISS', 'RETEEMIS'])
          } as ParceiroEmpresGrupoIcmsSankhya;
        });

        allRegistros = allRegistros.concat(registrosPagina);
        const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

        if (registrosPagina.length === 0 || !hasMoreResult) {
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
    return allRegistros;
  } catch (error: any) {
    if (retryCount < MAX_RETRIES - 1) {
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
      const novoToken = await obterToken(idSistema, true);
      return buscarParceiroEmpresGrupoIcmsSankhyaTotal(idSistema, novoToken, retryCount + 1);
    }
    throw error;
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_PARCEIRO_EMPRES_GRUPO_ICMS SET SANKHYA_ATUAL = 'N', DT_ULT_CARGA = CURRENT_TIMESTAMP WHERE ID_SISTEMA = :idSistema AND SANKHYA_ATUAL = 'S'`,
    [idSistema], { autoCommit: false }
  );
  return result.rowsAffected || 0;
}

/**
 * Upsert (inserir ou atualizar) registros usando MERGE
 */
async function upsertParceiroEmpresGrupoIcms(
  connection: oracledb.Connection,
  idSistema: number,
  registros: ParceiroEmpresGrupoIcmsSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;
  const BATCH_SIZE = 100;
  
  console.log(`💾 [Sync] Iniciando gravação de ${registros.length} registros no Oracle...`);

  for (let i = 0; i < registros.length; i += BATCH_SIZE) {
    const batch = registros.slice(i, i + BATCH_SIZE);
    for (const reg of batch) {
      // Aceitamos registros mesmo com chaves nulas agora que temos ID sequencial, 
      // mas o MERGE precisa de uma lógica especial para não duplicar.
      if (reg.CODEMP === null && reg.CODPARC === null && reg.CODTAB === null && reg.GRUPOICMS === null) {
        console.warn(`⚠️ [Sync] Pulando registro totalmente sem chaves:`, JSON.stringify(reg));
        continue;
      }

      try {
        const result = await connection.execute(
          `MERGE INTO AS_PARCEIRO_EMPRES_GRUPO_ICMS dest
           USING (SELECT :idSistema AS ID_SISTEMA, :codEmp AS CODEMP, :codParc AS CODPARC, :codTab AS CODTAB, :grupoIcms AS GRUPOICMS FROM DUAL) src
           ON (
             dest.ID_SISTEMA = src.ID_SISTEMA 
             AND (dest.CODEMP = src.CODEMP OR (dest.CODEMP IS NULL AND src.CODEMP IS NULL))
             AND (dest.CODPARC = src.CODPARC OR (dest.CODPARC IS NULL AND src.CODPARC IS NULL))
             AND (dest.CODTAB = src.CODTAB OR (dest.CODTAB IS NULL AND src.CODTAB IS NULL))
             AND (dest.GRUPOICMS = src.GRUPOICMS OR (dest.GRUPOICMS IS NULL AND src.GRUPOICMS IS NULL))
           )
           WHEN MATCHED THEN
             UPDATE SET CLASSIFICMS = :classificms, FORMULA = :formula, INDPRECOEMBUT = :indPrecoEmbut, RETEMISS = :retEmiss, SANKHYA_ATUAL = 'S', DT_ULT_CARGA = CURRENT_TIMESTAMP
           WHEN NOT MATCHED THEN
             INSERT (ID_SISTEMA, CODEMP, CODPARC, CODTAB, GRUPOICMS, CLASSIFICMS, FORMULA, INDPRECOEMBUT, RETEMISS, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO)
             VALUES (:idSistema, :codEmp, :codParc, :codTab, :grupoIcms, :classificms, :formula, :indPrecoEmbut, :retEmiss, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          {
            idSistema, 
            codEmp: reg.CODEMP === undefined ? null : reg.CODEMP, 
            codParc: reg.CODPARC === undefined ? null : reg.CODPARC, 
            codTab: reg.CODTAB === undefined ? null : reg.CODTAB, 
            grupoIcms: reg.GRUPOICMS === undefined ? null : reg.GRUPOICMS,
            classificms: reg.CLASSIFICMS || null,
            formula: reg.FORMULA || null,
            indPrecoEmbut: reg.INDPRECOEMBUT || null,
            retEmiss: reg.RETEMISS || null
          }, { autoCommit: false }
        );

        if (result.rowsAffected && result.rowsAffected > 0) {
          // Incremento genérico para simplificar
          inseridos++; 
        }
      } catch (err: any) {
        console.error(`❌ [Sync] Erro no registro:`, JSON.stringify(reg), err.message);
      }
    }
    await connection.commit();
    if (i % 1000 === 0 && i > 0) {
        console.log(`⏳ [Sync] Processados ${i} de ${registros.length} registros...`);
    }
  }
  return { inseridos, atualizados };
}

export async function sincronizarParceiroEmpresGrupoIcmsTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;
  try {
    const bearerToken = await obterToken(idSistema, true);
    const registros = await buscarParceiroEmpresGrupoIcmsSankhyaTotal(idSistema, bearerToken);
    connection = await getOracleConnection();
    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertParceiroEmpresGrupoIcms(connection, idSistema, registros);
    await connection.commit();
    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_PARCEIRO_EMPRES_GRUPO_ICMS', STATUS: 'SUCESSO',
      TOTAL_REGISTROS: registros.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    });
    return { success: true, idSistema, empresa: empresaNome, totalRegistros: registros.length, registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados, dataInicio, dataFim, duracao };
  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => { });
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_PARCEIRO_EMPRES_GRUPO_ICMS', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message, DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => { });
    return { success: false, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message };
  } finally {
    if (connection) await connection.close().catch(() => { });
  }
}

export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  const connection = await getOracleConnection();
  const result = await connection.execute(`SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  await connection.close();
  const resultados: SyncResult[] = [];
  const empresas = result.rows as any[];
  for (const empresa of empresas) {
    const resultado = await sincronizarParceiroEmpresGrupoIcmsTotal(empresa.ID_EMPRESA, empresa.EMPRESA);
    resultados.push(resultado);
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  return resultados;
}

export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
  const connection = await getOracleConnection();
  const query = idSistema
    ? `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_PARCEIRO_EMPRES_GRUPO_ICMS WHERE ID_SISTEMA = :idSistema GROUP BY ID_SISTEMA`
    : `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_PARCEIRO_EMPRES_GRUPO_ICMS GROUP BY ID_SISTEMA`;
  const result = await connection.execute(query, idSistema ? [idSistema] : [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
  await connection.close();
  return result.rows as any[];
}
