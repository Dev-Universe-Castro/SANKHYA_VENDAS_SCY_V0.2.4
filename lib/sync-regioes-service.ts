
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import { salvarLogSincronizacao, buscarDataUltimaSincronizacao } from './sync-logs-service';

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

interface RegiaoSankhya {
    CODREG: number;
    NOMEREG: string;
    ATIVA: string;
    ANALITICA: string;
    CODTAB?: number;
    CODTABMIN?: number;
    FRETEMIN?: number;
    PERCCUSVAR?: number;
    PERCDESCFOB?: number;
    CODVEND?: number;
    PERCADICIONAL?: number;
    PERCPREMIO?: number;
    GERARRECEITA?: string;
    VLRANTECIPACAO?: number;
    GRAU?: number;
    CODREGPAI?: number;
    VENDAMIN?: number;
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
 * Busca todas as regiões do Sankhya para uma empresa específica
 */
async function buscarRegioesSankhya(idSistema: number, bearerToken: string): Promise<RegiaoSankhya[]> {
    console.log(`🔍 [Sync] Buscando regiões do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allRegioes: RegiaoSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de regiões...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Regiao",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODREG, NOMEREG, ATIVA, ANALITICA, CODTAB, CODTABMIN, FRETEMIN, PERCCUSVAR, PERCDESCFOB, CODVEND, PERCADICIONAL, PERCPREMIO, GERARRECEITA, VLRANTECIPACAO, GRAU, CODREGPAI, VENDAMIN, DTALTER"
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

            if (respostaCompleta.status === '0') {
                // Sucesso
            } else if (respostaCompleta.statusMessage) {
                console.error(`❌ [Sync] Erro na API Sankhya: ${respostaCompleta.statusMessage}`);
            }

            const entities = respostaCompleta.responseBody?.entities;

            if (!entities || !entities.entity) {
                console.log(`⚠️ [Sync] Nenhuma região encontrada na página ${currentPage}`);
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const regioesPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as RegiaoSankhya;
            });

            allRegioes = allRegioes.concat(regioesPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${regioesPagina.length} registros (total acumulado: ${allRegioes.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (regioesPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allRegioes.length} regiões recuperadas`);
    return allRegioes;
}

/**
 * Marca todas como não atuais (soft delete)
 */
async function marcarTodasComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_REGIOES 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
        { idSistema },
        { autoCommit: false }
    );

    const rowsAffected = result.rowsAffected || 0;
    return rowsAffected;
}

/**
 * Executa UPSERT de regiões usando MERGE
 */
async function upsertRegioes(
    connection: oracledb.Connection,
    idSistema: number,
    regioes: RegiaoSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < regioes.length; i += BATCH_SIZE) {
        const batch = regioes.slice(i, i + BATCH_SIZE);

        for (const regiao of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_REGIOES dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codReg AS CODREG FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODREG = src.CODREG)
         WHEN MATCHED THEN
           UPDATE SET
             NOMEREG = :nomeReg,
             ATIVA = :ativa,
             ANALITICA = :analitica,
             CODTAB = :codTab,
             CODTABMIN = :codTabMin,
             FRETEMIN = :freteMin,
             PERCCUSVAR = :percCusVar,
             PERCDESCFOB = :percDescFob,
             CODVEND = :codVend,
             PERCADICIONAL = :percAdicional,
             PERCPREMIO = :percPremio,
             GERARRECEITA = :gerarReceita,
             VLRANTECIPACAO = :vlrAntecipacao,
             GRAU = :grau,
             CODREGPAI = :codRegPai,
             VENDAMIN = :vendaMin,
             DTALTER = :dtAlter,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODREG, NOMEREG, ATIVA, ANALITICA, CODTAB, CODTABMIN,
             FRETEMIN, PERCCUSVAR, PERCDESCFOB, CODVEND, PERCADICIONAL, PERCPREMIO,
             GERARRECEITA, VLRANTECIPACAO, GRAU, CODREGPAI, VENDAMIN, DTALTER,
             SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codReg, :nomeReg, :ativa, :analitica, :codTab, :codTabMin,
             :freteMin, :percCusVar, :percDescFob, :codVend, :percAdicional, :percPremio,
             :gerarReceita, :vlrAntecipacao, :grau, :codRegPai, :vendaMin, :dtAlter,
             'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codReg: regiao.CODREG || null,
                    nomeReg: regiao.NOMEREG || null,
                    ativa: regiao.ATIVA || null,
                    analitica: regiao.ANALITICA || null,
                    codTab: regiao.CODTAB || null,
                    codTabMin: regiao.CODTABMIN || null,
                    freteMin: regiao.FRETEMIN || null,
                    percCusVar: regiao.PERCCUSVAR || null,
                    percDescFob: regiao.PERCDESCFOB || null,
                    codVend: regiao.CODVEND || null,
                    percAdicional: regiao.PERCADICIONAL || null,
                    percPremio: regiao.PERCPREMIO || null,
                    gerarReceita: regiao.GERARRECEITA || null,
                    vlrAntecipacao: regiao.VLRANTECIPACAO || null,
                    grau: regiao.GRAU || null,
                    codRegPai: regiao.CODREGPAI || null,
                    vendaMin: regiao.VENDAMIN || null,
                    dtAlter: (function () {
                        if (!regiao.DTALTER) return null;
                        try {
                            const partes = regiao.DTALTER.trim().split(' ');
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
                    `SELECT DT_CRIACAO FROM AS_REGIOES WHERE ID_SISTEMA = :idSistema AND CODREG = :codReg`,
                    { idSistema, codReg: regiao.CODREG },
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
    }

    return { inseridos, atualizados };
}

/**
 * Sincroniza regiões de uma empresa específica
 */
export async function sincronizarRegioesPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        const bearerToken = await obterToken(idSistema, true);
        const regioes = await buscarRegioesSankhya(idSistema, bearerToken);

        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodasComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertRegioes(connection, idSistema, regioes);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        try {
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_REGIOES',
                STATUS: 'SUCESSO',
                TOTAL_REGISTROS: regioes.length,
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
            totalRegistros: regioes.length,
            registrosInseridos: inseridos,
            registrosAtualizados: atualizados,
            registrosDeletados,
            dataInicio,
            dataFim,
            duracao
        };

    } catch (error: any) {
        console.error(`❌ [Sync] Erro ao sincronizar regiões para ${empresaNome}:`, error);

        if (connection) {
            await connection.rollback().catch(() => { });
        }

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        try {
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_REGIOES',
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
        if (connection) await connection.close().catch(() => { });
    }
}

/**
 * Busca regiões do Sankhya alteradas (SINCRONIZAÇÃO PARCIAL)
 */
export async function buscarRegioesSankhyaParcial(
    idSistema: number,
    bearerToken: string,
    dataUltimaSync: Date
): Promise<RegiaoSankhya[]> {
    const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
    const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
    const ano = dataUltimaSync.getFullYear();
    const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
    const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
    const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}`;

    console.log(`🔍 [Sync] [Parcial] Buscando regiões alteradas desde ${dataFormatada}`);

    let allRegioes: RegiaoSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    while (hasMoreData) {
        const payload = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Regiao",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "criteria": { "expression": { "$": `DTALTER > TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI')` } },
                    "entity": { "fieldset": { "list": "CODREG, NOMEREG, ATIVA, ANALITICA, CODTAB, CODTABMIN, FRETEMIN, PERCCUSVAR, PERCDESCFOB, CODVEND, PERCADICIONAL, PERCPREMIO, GERARRECEITA, VLRANTECIPACAO, GRAU, CODREGPAI, VENDAMIN, DTALTER" } }
                }
            }
        };

        const contratto = await buscarContrato(idSistema);
        if (!contratto) throw new Error(`Contrato ${idSistema} não encontrado`);
        const baseUrl = contratto.IS_SANDBOX ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
        const url = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

        const axios = require('axios');
        try {
            const response = await axios.post(url, payload, {
                headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
                timeout: 60000
            });
            const entities = response.data.responseBody?.entities;
            if (!entities || !entities.entity) break;
            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];
            const regioesPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    if (rawEntity[fieldKey]) cleanObject[fieldNames[i]] = rawEntity[fieldKey].$;
                }
                return cleanObject as RegiaoSankhya;
            });
            allRegioes = allRegioes.concat(regioesPagina);
            if (regioesPagina.length === 0 || !(entities.hasMoreResult === 'true' || entities.hasMoreResult === true)) {
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
    return allRegioes;
}

/**
 * Sincronização parcial de regiões
 */
export async function sincronizarRegioesParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;
    try {
        const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_REGIOES');
        if (!dataUltimaSync) return sincronizarRegioesPorEmpresa(idSistema, empresaNome);

        const bearerToken = await obterToken(idSistema, true);
        const regioes = await buscarRegioesSankhyaParcial(idSistema, bearerToken, dataUltimaSync);
        if (regioes.length === 0) {
            return {
                success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
                registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
                dataInicio, dataFim: new Date(), duracao: 0
            };
        }
        connection = await getOracleConnection();
        const { inseridos, atualizados } = await upsertRegioes(connection, idSistema, regioes);
        await connection.commit();
        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();
        try {
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_REGIOES', STATUS: 'SUCESSO',
                TOTAL_REGISTROS: regioes.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
                REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
            });
        } catch { }
        return {
            success: true, idSistema, empresa: empresaNome, totalRegistros: regioes.length,
            registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
            dataInicio, dataFim, duracao
        };
    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        try {
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_REGIOES', STATUS: 'FALHA',
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
 * Sincroniza todas as regiões de todas as empresas
 */
export async function sincronizarTodasRegioes(): Promise<SyncResult[]> {
    const resultados: SyncResult[] = [];
    const connection = await getOracleConnection();
    try {
        const result = await connection.execute(
            `SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await connection.close();
        if (!result.rows) return [];
        const empresas = result.rows as any[];
        for (const empresa of empresas) {
            const res = await sincronizarRegioesPorEmpresa(empresa.ID_EMPRESA, empresa.EMPRESA);
            resultados.push(res);
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        return resultados;
    } catch (error) {
        if (connection) await connection.close().catch(() => { });
        throw error;
    }
}

/**
 * Obter estatísticas
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
            FROM AS_REGIOES
            ${whereClause}
            GROUP BY ID_SISTEMA`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        return result.rows || [];
    } finally {
        await connection.close();
    }
}

/**
 * Listar regiões
 */
export async function listarRegioes(idSistema?: number) {
    const connection = await getOracleConnection();
    try {
        const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};
        const result = await connection.execute(
            `SELECT * FROM AS_REGIOES ${whereClause} ORDER BY NOMEREG`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        return result.rows || [];
    } finally {
        await connection.close();
    }
}
