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

interface GrupoProdutoSankhya {
    CODGRUPOPROD: number;
    DESCRGRUPOPROD: string;
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
 * Busca todos os grupos de produtos do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
async function buscarGruposProdutosSankhyaTotal(idSistema: number, bearerToken: string): Promise<GrupoProdutoSankhya[]> {
    console.log(`🔍 [Sync] Buscando grupos de produtos do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allGrupos: GrupoProdutoSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de grupos de produtos...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "GrupoProduto",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODGRUPOPROD, DESCRGRUPOPROD"
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

            if (respostaCompleta.status === '0') {
                // Sucesso
            } else if (respostaCompleta.statusMessage) {
                console.error(`❌ [Sync] Erro na API Sankhya: ${respostaCompleta.statusMessage}`);
            }

            const entities = respostaCompleta.responseBody?.entities;

            if (!entities || !entities.entity) {
                console.log(`⚠️ [Sync] Nenhum grupo de produto encontrado na página ${currentPage}`);
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const gruposPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as GrupoProdutoSankhya;
            });

            allGrupos = allGrupos.concat(gruposPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${gruposPagina.length} registros (total acumulado: ${allGrupos.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (gruposPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${gruposPagina.length})`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                console.log(`📊 [Sync] Progresso mantido: ${allGrupos.length} registros acumulados`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allGrupos.length} grupos de produtos recuperados em ${currentPage} páginas`);
    return allGrupos;
}

/**
 * Busca grupos de produtos do Sankhya alterados após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarGruposProdutosSankhyaParcial(idSistema: number, bearerToken: string, dataUltimaSync: Date): Promise<GrupoProdutoSankhya[]> {
    const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
    const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
    const ano = dataUltimaSync.getFullYear();
    const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
    const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
    const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
    const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

    console.log(`🔍 [Sync] [Parcial] Buscando grupos de produtos alterados desde ${dataFormatada}`);

    let allGrupos: GrupoProdutoSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    while (hasMoreData) {
        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "GrupoProduto",
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
                            "list": "CODGRUPOPROD, DESCRGRUPOPROD"
                        }
                    }
                }
            }
        };

        const contrato = await buscarContrato(idSistema);
        if (!contrato) throw new Error(`Contrato ${idSistema} não encontrado`);
        const baseUrl = contrato.IS_SANDBOX ? "https://api.sandbox.sankhya.com.br" : "https://api.sankhya.com.br";
        const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

        const axios = require('axios');

        try {
            const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
                headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
                timeout: 60000
            });

            const entities = response.data.responseBody?.entities;
            if (!entities || !entities.entity) break;

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const gruposPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
                }
                return cleanObject as GrupoProdutoSankhya;
            });

            allGrupos = allGrupos.concat(gruposPagina);
            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (gruposPagina.length === 0 || !hasMoreResult) {
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
    return allGrupos;
}

/**
 * Sincronização Parcial de grupos de produtos
 */
export async function sincronizarGruposProdutosParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀 [Sync] [Parcial] GRUPOS DE PRODUTOS: ${empresaNome}`);
        const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_GRUPOS_PRODUTOS');

        if (!dataUltimaSync) return sincronizarGruposProdutosTotal(idSistema, empresaNome);

        const bearerToken = await obterToken(idSistema, true);
        const grupos = await buscarGruposProdutosSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

        if (grupos.length === 0) {
            return {
                success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
                registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
                dataInicio, dataFim: new Date(), duracao: 0
            };
        }

        connection = await getOracleConnection();
        const { inseridos, atualizados } = await upsertGruposProdutos(connection, idSistema, grupos);
        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_GRUPOS_PRODUTOS', STATUS: 'SUCESSO',
            TOTAL_REGISTROS: grupos.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        });

        return {
            success: true, idSistema, empresa: empresaNome, totalRegistros: grupos.length,
            registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
            dataInicio, dataFim, duracao
        };
    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_GRUPOS_PRODUTOS', STATUS: 'FALHA',
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
 * Executa o soft delete (marca como não atual) todos os grupos de produtos do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_GRUPOS_PRODUTOS 
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
 * Executa UPSERT de grupos de produtos usando MERGE
 */
async function upsertGruposProdutos(
    connection: oracledb.Connection,
    idSistema: number,
    grupos: GrupoProdutoSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < grupos.length; i += BATCH_SIZE) {
        const batch = grupos.slice(i, i + BATCH_SIZE);

        for (const grupo of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_GRUPOS_PRODUTOS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codGrupoProd AS CODGRUPOPROD FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODGRUPOPROD = src.CODGRUPOPROD)
         WHEN MATCHED THEN
           UPDATE SET
             DESCRGRUPOPROD = :descrGrupoProd,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODGRUPOPROD, DESCRGRUPOPROD, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codGrupoProd, :descrGrupoProd, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codGrupoProd: grupo.CODGRUPOPROD || null,
                    descrGrupoProd: grupo.DESCRGRUPOPROD || null
                },
                { autoCommit: false }
            );

            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_GRUPOS_PRODUTOS 
            WHERE ID_SISTEMA = :idSistema AND CODGRUPOPROD = :codGrupoProd`,
                    { idSistema, codGrupoProd: grupo.CODGRUPOPROD },
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
        console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(grupos.length / BATCH_SIZE)}`);
    }

    console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
    return { inseridos, atualizados };
}

/**
 * Sincronização Total de grupos de produtos
 */
export async function sincronizarGruposProdutosTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀🚀🚀 ================================================`);
        console.log(`🚀 SINCRONIZAÇÃO DE GRUPOS DE PRODUTOS`);
        console.log(`🚀 ID_SISTEMA: ${idSistema}`);
        console.log(`🚀 Empresa: ${empresaNome}`);
        console.log(`🚀 ================================================\n`);

        // SEMPRE forçar renovação do token
        console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
        let bearerToken = await obterToken(idSistema, true);
        const grupos = await buscarGruposProdutosSankhyaTotal(idSistema, bearerToken);
        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertGruposProdutos(connection, idSistema, grupos);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
        console.log(`📊 [Sync] Resumo: ${grupos.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
        console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

        // Salvar log de sucesso
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_GRUPOS_PRODUTOS',
            STATUS: 'SUCESSO',
            TOTAL_REGISTROS: grupos.length,
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
            totalRegistros: grupos.length,
            registrosInseridos: inseridos,
            registrosAtualizados: atualizados,
            registrosDeletados,
            dataInicio,
            dataFim,
            duracao
        };

    } catch (error: any) {
        console.error(`❌ [Sync] Erro ao sincronizar grupos de produtos para ${empresaNome}:`, error);

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
            TABELA: 'AS_GRUPOS_PRODUTOS',
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
 * Sincroniza grupos de produtos de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
    console.log('🌐 [Sync] Iniciando sincronização de grupos de produtos de todas as empresas...');

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
            const resultado = await sincronizarGruposProdutosTotal(
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
       FROM AS_GRUPOS_PRODUTOS
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
 * Listar grupos de produtos sincronizados (Para o frontend)
 */
export async function listarGruposProdutos(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE GP.ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
                GP.ID_SISTEMA,
                AC.EMPRESA as NOME_CONTRATO,
                GP.CODGRUPOPROD,
                GP.DESCRGRUPOPROD,
                GP.SANKHYA_ATUAL,
                GP.DT_ULT_CARGA
            FROM AS_GRUPOS_PRODUTOS GP
            JOIN AD_CONTRATOS AC ON AC.ID_EMPRESA = GP.ID_SISTEMA
            ${whereClause}
            ORDER BY GP.ID_SISTEMA, GP.DESCRGRUPOPROD`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}
