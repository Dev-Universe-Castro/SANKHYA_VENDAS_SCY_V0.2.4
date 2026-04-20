
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

interface TabelaPreco {
    NUTAB: number;
    DTVIGOR: string;
    PERCENTUAL?: number;
    UTILIZADECCUSTO?: string;
    CODTABORIG?: number;
    DTALTER: string;
    CODTAB?: number;
    JAPE_ID?: string;
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
 * Busca todas as tabelas de preços do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
export async function buscarTabelaPrecosSankhyaTotal(
    idSistema: number,
    bearerToken: string,
    retryCount: number = 0
): Promise<TabelaPreco[]> {
    const MAX_RETRIES = 3;
    const RETRY_DELAY = 2000;

    let allTabelas: TabelaPreco[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    try {
        while (hasMoreData) {
            const payload = {
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "TabelaPreco",
                        "includePresentationFields": "N",
                        "useFileBasedPagination": true,
                        "disableRowsLimit": true,
                        "offsetPage": currentPage.toString(),
                        "entity": {
                            "fieldset": {
                                "list": "NUTAB, DTVIGOR, PERCENTUAL, UTILIZADECCUSTO, CODTABORIG, DTALTER, CODTAB, JAPE_ID"
                            }
                        }
                    }
                }
            };

            const contrato = await buscarContrato(idSistema);
            if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
            const baseUrl = contrato.IS_SANDBOX === true ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
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

                const tabelasPagina = entityArray.map((rawEntity: any) => {
                    const cleanObject: any = {};
                    for (let i = 0; i < fieldNames.length; i++) {
                        const fieldKey = `f${i}`;
                        if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
                    }
                    return cleanObject as TabelaPreco;
                });

                allTabelas = allTabelas.concat(tabelasPagina);
                if (tabelasPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
                    hasMoreData = false;
                } else {
                    currentPage++;
                    await new Promise(resolve => setTimeout(resolve, 500));
                }
            } catch (pageError: any) {
                if (pageError.response?.status === 401 || pageError.response?.status === 403) {
                    currentToken = await obterToken(idSistema, true);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                } else throw pageError;
            }
        }
        return allTabelas;
    } catch (error: any) {
        if (retryCount < MAX_RETRIES - 1) {
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
            const novoToken = await obterToken(idSistema, true);
            return buscarTabelaPrecosSankhyaTotal(idSistema, novoToken, retryCount + 1);
        }
        throw error;
    }
}

/**
 * Busca tabelas de preços do Sankhya alteradas (SINCRONIZAÇÃO PARCIAL)
 */
export async function buscarTabelaPrecosSankhyaParcial(
    idSistema: number,
    bearerToken: string,
    dataUltimaSync: Date
): Promise<TabelaPreco[]> {
    const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
    const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
    const ano = dataUltimaSync.getFullYear();
    const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
    const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
    const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
    const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

    let allTabelas: TabelaPreco[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    while (hasMoreData) {
        const payload = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "TabelaPreco",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "criteria": { "expression": { "$": `DTALTER >= TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI:SS')` } },
                    "entity": { "fieldset": { "list": "NUTAB, DTVIGOR, PERCENTUAL, UTILIZADECCUSTO, CODTABORIG, DTALTER, CODTAB, JAPE_ID" } }
                }
            }
        };

        const contrato = await buscarContrato(idSistema);
        if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
        const baseUrl = contrato.IS_SANDBOX === true ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
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
            const tabelasPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
                }
                return cleanObject as TabelaPreco;
            });
            allTabelas = allTabelas.concat(tabelasPagina);
            if (tabelasPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
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
    return allTabelas;
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_TABELA_PRECOS SET SANKHYA_ATUAL = 'N', DT_ULT_CARGA = CURRENT_TIMESTAMP WHERE ID_SISTEMA = :idSistema AND SANKHYA_ATUAL = 'S'`,
        [idSistema], { autoCommit: false }
    );
    return result.rowsAffected || 0;
}

/**
 * Upsert (inserir ou atualizar) tabelas de preços usando MERGE
 */
async function upsertTabelasPrecos(
    connection: oracledb.Connection,
    idSistema: number,
    tabelas: TabelaPreco[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;
    const BATCH_SIZE = 100;
    for (let i = 0; i < tabelas.length; i += BATCH_SIZE) {
        const batch = tabelas.slice(i, i + BATCH_SIZE);
        for (const tabela of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_TABELA_PRECOS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :nutab AS NUTAB FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.NUTAB = src.NUTAB)
         WHEN MATCHED THEN
           UPDATE SET DTVIGOR = :dtVigor, PERCENTUAL = :percentual, UTILIZADECCUSTO = :utilizaDecCusto, CODTABORIG = :codTabOrig, DTALTER = :dtAlter, CODTAB = :codTab, JAPE_ID = :japeId, SANKHYA_ATUAL = 'S', DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (ID_SISTEMA, NUTAB, DTVIGOR, PERCENTUAL, UTILIZADECCUSTO, CODTABORIG, DTALTER, CODTAB, JAPE_ID, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO)
           VALUES (:idSistema, :nutab, :dtVigor, :percentual, :utilizaDecCusto, :codTabOrig, :dtAlter, :codTab, :japeId, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
                {
                    idSistema, nutab: tabela.NUTAB, dtVigor: parseDataSankhya(tabela.DTVIGOR),
                    percentual: tabela.PERCENTUAL || null, utilizaDecCusto: tabela.UTILIZADECCUSTO || null,
                    codTabOrig: tabela.CODTABORIG || null, dtAlter: parseDataSankhya(tabela.DTALTER),
                    codTab: tabela.CODTAB || null, japeId: tabela.JAPE_ID || null
                }, { autoCommit: false }
            );
            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_TABELA_PRECOS WHERE ID_SISTEMA = :idSistema AND NUTAB = :nutab`,
                    { idSistema, nutab: tabela.NUTAB }, { outFormat: oracledb.OUT_FORMAT_OBJECT }
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

export async function sincronizarTabelaPrecosTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;
    try {
        const bearerToken = await obterToken(idSistema, true);
        const tabelas = await buscarTabelaPrecosSankhyaTotal(idSistema, bearerToken);
        connection = await getOracleConnection();
        const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertTabelasPrecos(connection, idSistema, tabelas);
        await connection.commit();
        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TABELA_PRECOS', STATUS: 'SUCESSO',
            TOTAL_REGISTROS: tabelas.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: registrosDeletados, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        });
        return { success: true, idSistema, empresa: empresaNome, totalRegistros: tabelas.length, registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados, dataInicio, dataFim, duracao };
    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TABELA_PRECOS', STATUS: 'FALHA',
            TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
            DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        }).catch(() => { });
        return { success: false, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message };
    } finally {
        if (connection) await connection.close().catch(() => { });
    }
}

export async function sincronizarTabelaPrecosParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;
    try {
        const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_TABELA_PRECOS');
        if (!dataUltimaSync) return sincronizarTabelaPrecosTotal(idSistema, empresaNome);
        const bearerToken = await obterToken(idSistema, true);
        const tabelas = await buscarTabelaPrecosSankhyaParcial(idSistema, bearerToken, dataUltimaSync);
        if (tabelas.length === 0) return { success: true, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim: new Date(), duracao: 0 };
        connection = await getOracleConnection();
        const { inseridos, atualizados } = await upsertTabelasPrecos(connection, idSistema, tabelas);
        await connection.commit();
        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TABELA_PRECOS', STATUS: 'SUCESSO',
            TOTAL_REGISTROS: tabelas.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        });
        return { success: true, idSistema, empresa: empresaNome, totalRegistros: tabelas.length, registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0, dataInicio, dataFim, duracao };
    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_TABELA_PRECOS', STATUS: 'FALHA',
            TOTAL_REGISTROS: 0, REGISTROS_INSERIDOS: 0, REGISTROS_ATUALIZADOS: 0, REGISTROS_DELETADOS: 0,
            DURACAO_MS: dataFim.getTime() - dataInicio.getTime(), MENSAGEM_ERRO: error.message, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        }).catch(() => { });
        return { success: false, idSistema, empresa: empresaNome, totalRegistros: 0, registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0, dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime(), erro: error.message };
    } finally {
        if (connection) await connection.close().catch(() => { });
    }
}

export async function sincronizarTabelaPrecosPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
    return sincronizarTabelaPrecosTotal(idSistema, empresaNome);
}

export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
    const connection = await getOracleConnection();
    const result = await connection.execute(`SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
    await connection.close();
    const resultados: SyncResult[] = [];
    const empresas = result.rows as any[];
    for (const empresa of empresas) {
        const resultado = await sincronizarTabelaPrecosTotal(empresa.ID_EMPRESA, empresa.EMPRESA);
        resultados.push(resultado);
        await new Promise(resolve => setTimeout(resolve, 2000));
    }
    return resultados;
}

export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
    const connection = await getOracleConnection();
    const query = idSistema
        ? `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_TABELA_PRECOS WHERE ID_SISTEMA = :idSistema GROUP BY ID_SISTEMA`
        : `SELECT ID_SISTEMA, COUNT(*) as TOTAL_REGISTROS, SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS, SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS, MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO FROM AS_TABELA_PRECOS GROUP BY ID_SISTEMA`;
    const result = await connection.execute(query, idSistema ? [idSistema] : [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
    await connection.close();
    return result.rows as any[];
}

export async function listarTabelaPrecos(idSistema?: number) {
    const connection = await getOracleConnection();
    try {
        const whereClause = idSistema ? `WHERE T.ID_SISTEMA = :idSistema` : '';
        const result = await connection.execute(`SELECT T.ID_SISTEMA, C.EMPRESA as NOME_CONTRATO, T.NUTAB, T.DTVIGOR, T.PERCENTUAL, T.SANKHYA_ATUAL, T.DT_ULT_CARGA FROM AS_TABELA_PRECOS T JOIN AD_CONTRATOS C ON C.ID_EMPRESA = T.ID_SISTEMA ${whereClause} ORDER BY T.ID_SISTEMA, T.NUTAB FETCH FIRST 500 ROWS ONLY`, idSistema ? { idSistema } : {}, { outFormat: oracledb.OUT_FORMAT_OBJECT });
        return result.rows || [];
    } finally { await connection.close(); }
}
