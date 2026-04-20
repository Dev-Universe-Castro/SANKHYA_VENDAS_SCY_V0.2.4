import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';
import { salvarLogSincronizacao } from './sync-logs-service';

/**
 * Interface para os dados de Volume Alternativo do Sankhya (ProdutoUnidade)
 */
interface VolumeAlternativoSankhya {
  CODPROD: number;
  CODVOL: string;
  ATIVO?: string;
  CAMADAS?: number;
  CODBARRA?: string;
  CONTROLE?: string;
  DESCRDANFE?: string;
  DESCRUNTRIBEXPORT?: string;
  DIVIDEMULTIPLICA?: string;
  LASTRO?: number;
  M3?: number;
  MULTIPVLR?: number;
  OPCAOSEP?: string;
  OPCOESGERAR0220?: string;
  QTDDECIMAISUPF?: number;
  QUANTIDADE?: number;
  SELECIONADO?: string;
  TIPCODBARRA?: string;
  TIPGTINNFE?: number;
  UNDTRIBRECOB?: string;
  UNIDSELO?: string;
  UNIDTRIB?: string;
  UNTRIBEXPORTACAO?: string;
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
 * Busca volumes alternativos do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
async function buscarVolumesSankhyaTotal(idSistema: number, bearerToken: string): Promise<VolumeAlternativoSankhya[]> {
  console.log(`🔍 [Sync] Buscando volumes alternativos do Sankhya...`);

  let allVolumes: VolumeAlternativoSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;

  while (hasMoreData) {
    console.log(`📄 [Sync] Buscando página ${currentPage} de volumes alternativos...`);

    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "VolumeAlternativo",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "entity": {
            "fieldset": {
              "list": "*" // Buscando todos os campos mapeados na entity
            }
          }
        }
      }
    };

    const { buscarContratoPorId } = await import('./oracle-service');
    const contrato = await buscarContratoPorId(idSistema);
    const isSandbox = contrato?.IS_SANDBOX === true;
    const baseUrl = isSandbox ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
    const URL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    try {
      const response = await axios.post(URL, PAYLOAD, {
        headers: {
          'Authorization': `Bearer ${bearerToken}`,
          'Content-Type': 'application/json'
        },
        timeout: 60000
      });

      const entities = response.data.responseBody?.entities;
      if (!entities || !entities.entity) {
        hasMoreData = false;
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const volumesPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as VolumeAlternativoSankhya;
      });

      allVolumes = allVolumes.concat(volumesPagina);
      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (volumesPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro na página ${currentPage}:`, error.message);
      throw error;
    }
  }

  return allVolumes;
}

/**
 * Executa UPSERT no Oracle
 */
async function upsertVolumesAlternativos(
  connection: oracledb.Connection,
  idSistema: number,
  volumes: VolumeAlternativoSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;
  const BATCH_SIZE = 100;

  for (let i = 0; i < volumes.length; i += BATCH_SIZE) {
    const batch = volumes.slice(i, i + BATCH_SIZE);

    for (const vol of batch) {
      try {
        const result = await connection.execute(
          `MERGE INTO AS_VOLUME_ALTERNATIVO dest
           USING (SELECT :idSistema AS ID_SISTEMA, :codProd AS CODPROD, :codVol AS CODVOL FROM DUAL) src
           ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPROD = src.CODPROD AND dest.CODVOL = src.CODVOL)
           WHEN MATCHED THEN
             UPDATE SET
               ATIVO = :ativo,
               CAMADAS = :camadas,
               CODBARRA = :codBarra,
               CONTROLE = :controle,
               DESCRDANFE = :descrDanfe,
               DESCRUNTRIBEXPORT = :descrUntribExport,
               DIVIDEMULTIPLICA = :divideMultiplica,
               LASTRO = :lastro,
               M3 = :m3,
               MULTIPVLR = :multiVlr,
               OPCAOSEP = :opcaoSep,
               OPCOESGERAR0220 = :opcoesGerar0220,
               QTDDECIMAISUPF = :qtdDecimaisUpf,
               QUANTIDADE = :quantidade,
               SELECIONADO = :selecionado,
               TIPCODBARRA = :tipCodBarra,
               TIPGTINNFE = :tipGtinNfe,
               UNDTRIBRECOB = :undTribRecob,
               UNIDSELO = :unidSelo,
               UNIDTRIB = :unidTrib,
               UNTRIBEXPORTACAO = :unidTribExportacao,
               SANKHYA_ATUAL = 'S',
               DT_ULT_CARGA = CURRENT_TIMESTAMP
           WHEN NOT MATCHED THEN
             INSERT (
               ID_SISTEMA, CODPROD, CODVOL, ATIVO, CAMADAS, CODBARRA, CONTROLE,
               DESCRDANFE, DESCRUNTRIBEXPORT, DIVIDEMULTIPLICA, LASTRO, M3,
               MULTIPVLR, OPCAOSEP, OPCOESGERAR0220, QTDDECIMAISUPF, QUANTIDADE,
               SELECIONADO, TIPCODBARRA, TIPGTINNFE, UNDTRIBRECOB, UNIDSELO,
               UNIDTRIB, UNTRIBEXPORTACAO, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
             )
             VALUES (
               :idSistema, :codProd, :codVol, :ativo, :camadas, :codBarra, :controle,
               :descrDanfe, :descrUntribExport, :divideMultiplica, :lastro, :m3,
               :multiVlr, :opcaoSep, :opcoesGerar0220, :qtdDecimaisUpf, :quantidade,
               :selecionado, :tipCodBarra, :tipGtinNfe, :undTribRecob, :unidSelo,
               :unidTrib, :unidTribExportacao, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
             )`,
          {
            idSistema,
            codProd: vol.CODPROD ? String(vol.CODPROD) : null,
            codVol: vol.CODVOL || null,
            ativo: vol.ATIVO || 'S',
            camadas: vol.CAMADAS || null,
            codBarra: vol.CODBARRA || null,
            controle: vol.CONTROLE || null,
            descrDanfe: vol.DESCRDANFE || null,
            descrUntribExport: vol.DESCRUNTRIBEXPORT || null,
            divideMultiplica: vol.DIVIDEMULTIPLICA || null,
            lastro: vol.LASTRO || null,
            m3: vol.M3 || null,
            multiVlr: vol.MULTIPVLR || null,
            opcaoSep: vol.OPCAOSEP || null,
            opcoesGerar0220: vol.OPCOESGERAR0220 || null,
            qtdDecimaisUpf: vol.QTDDECIMAISUPF || null,
            quantidade: vol.QUANTIDADE || null,
            selecionado: vol.SELECIONADO || null,
            tipCodBarra: vol.TIPCODBARRA || null,
            tipGtinNfe: vol.TIPGTINNFE || null,
            undTribRecob: vol.UNDTRIBRECOB || null,
            unidSelo: vol.UNIDSELO || null,
            unidTrib: vol.UNIDTRIB || null,
            unidTribExportacao: vol.UNTRIBEXPORTACAO || null
          },
          { autoCommit: false }
        );

        if (result.rowsAffected && result.rowsAffected > 0) {
            inseridos++; // MERGE não diferencia facilmente sem query extra, incrementamos genérico
        }
      } catch (err: any) {
        console.error(`❌ [Sync] Erro no registro PROD:${vol.CODPROD} VOL:${vol.CODVOL}:`, err.message);
      }
    }
    await connection.commit();
  }

  return { inseridos, atualizados };
}

/**
 * Função principal de sincronização
 */
export async function sincronizarVolumesAlternativosTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀 Iniciando sincronização de Volumes Alternativos: ${empresaNome}`);
    const bearerToken = await obterToken(idSistema, true);
    const volumes = await buscarVolumesSankhyaTotal(idSistema, bearerToken);

    connection = await getOracleConnection();

    // Soft delete de registros antigos
    await connection.execute(
      `UPDATE AS_VOLUME_ALTERNATIVO SET SANKHYA_ATUAL = 'N' WHERE ID_SISTEMA = :idSistema`,
      { idSistema },
      { autoCommit: false }
    );

    const { inseridos, atualizados } = await upsertVolumesAlternativos(connection, idSistema, volumes);
    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_VOLUME_ALTERNATIVO',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: volumes.length,
      REGISTROS_INSERIDOS: inseridos,
      REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: 0,
      DURACAO_MS: duracao,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    });

    return {
      success: true, idSistema, empresa: empresaNome, totalRegistros: volumes.length,
      registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
      dataInicio, dataFim, duracao
    };

  } catch (error: any) {
    if (connection) await connection.rollback().catch(() => {});
    const dataFim = new Date();
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_VOLUME_ALTERNATIVO', STATUS: 'FALHA',
      TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
      DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message,
      DATA_INICIO: dataInicio, DATA_FIM: dataFim
    }).catch(() => {});

    return {
      success: false, idSistema, empresa: empresaNome, totalRegistros: 0,
      registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
      dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message
    };
  } finally {
    if (connection) await connection.close().catch(() => {});
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
       FROM AS_VOLUME_ALTERNATIVO
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
 * Listar volumes sincronizados
 */
export async function listarVolumes(idSistema?: number) {
  const connection = await getOracleConnection();
  try {
    const whereClause = idSistema ? `WHERE V.ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
         V.ID_SISTEMA,
         V.CODPROD,
         V.CODVOL,
         V.ATIVO,
         V.QUANTIDADE,
         V.SANKHYA_ATUAL,
         V.DT_ULT_CARGA,
         C.EMPRESA as NOME_CONTRATO
       FROM AS_VOLUME_ALTERNATIVO V
       JOIN AD_CONTRATOS C ON V.ID_SISTEMA = C.ID_EMPRESA
       ${whereClause}
       ORDER BY V.DT_ULT_CARGA DESC
       FETCH FIRST 500 ROWS ONLY`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}
