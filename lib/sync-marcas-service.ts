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

interface MarcaSankhya {
    CODIGO: number;
    DESCRICAO: string;
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
 * Busca todas as marcas do Sankhya para uma empresa específica (SINCRONIZAÇÃO TOTAL)
 */
async function buscarMarcasSankhyaTotal(idSistema: number, bearerToken: string): Promise<MarcaSankhya[]> {
    console.log(`🔍 [Sync] Buscando marcas do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allMarcas: MarcaSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de marcas...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "MarcaProduto",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODIGO, DESCRICAO"
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
                // Sucesso, mas pode não ter entidades
            } else if (respostaCompleta.statusMessage) {
                console.error(`❌ [Sync] Erro na API Sankhya: ${respostaCompleta.statusMessage}`);
                if (respostaCompleta.statusMessage.includes('não encontrada')) {
                    //
                }
            }

            const entities = respostaCompleta.responseBody?.entities;

            if (!entities || !entities.entity) {
                console.log(`⚠️ [Sync] Nenhuma marca encontrada na página ${currentPage}`);
                // Debug response
                console.log('DEBUG Response:', JSON.stringify(respostaCompleta).substring(0, 500));
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const marcasPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as MarcaSankhya;
            });

            allMarcas = allMarcas.concat(marcasPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${marcasPagina.length} registros (total acumulado: ${allMarcas.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (marcasPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${marcasPagina.length})`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                console.log(`📊 [Sync] Progresso mantido: ${allMarcas.length} registros acumulados`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allMarcas.length} marcas recuperadas em ${currentPage} páginas`);
    return allMarcas;
}

/**
 * Busca marcas do Sankhya alteradas após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarMarcasSankhyaParcial(idSistema: number, bearerToken: string, dataUltimaSync: Date): Promise<MarcaSankhya[]> {
    const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
    const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
    const ano = dataUltimaSync.getFullYear();
    const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
    const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
    const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}`;

    console.log(`🔍 [Sync] [Parcial] Buscando marcas alteradas desde ${dataFormatada}`);

    let allMarcas: MarcaSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    while (hasMoreData) {
        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "MarcaProduto",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "criteria": {
                        "expression": {
                            "$": `DHALTREG > TO_DATE('${dataFormatada}', 'DD/MM/YYYY HH24:MI')`
                        }
                    },
                    "entity": {
                        "fieldset": {
                            "list": "CODIGO, DESCRICAO"
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

        try {
            const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
                headers: { 'Authorization': `Bearer ${currentToken}`, 'Content-Type': 'application/json' },
                timeout: 60000
            });

            const entities = response.data.responseBody?.entities;
            if (!entities || !entities.entity) break;

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const marcasPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
                }
                return cleanObject as MarcaSankhya;
            });

            allMarcas = allMarcas.concat(marcasPagina);
            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (marcasPagina.length === 0 || !hasMoreResult) {
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

    return allMarcas;
}

/**
 * Executa o soft delete (marca como não atual) todas as marcas do sistema
 */
async function marcarTodasComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_MARCAS 
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
 * Executa UPSERT de marcas usando MERGE
 */
async function upsertMarcas(
    connection: oracledb.Connection,
    idSistema: number,
    marcas: MarcaSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < marcas.length; i += BATCH_SIZE) {
        const batch = marcas.slice(i, i + BATCH_SIZE);

        for (const marca of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_MARCAS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codigo AS CODIGO FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODIGO = src.CODIGO)
         WHEN MATCHED THEN
           UPDATE SET
             DESCRICAO = :descricao,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODIGO, DESCRICAO, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codigo, :descricao, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codigo: marca.CODIGO || null,
                    descricao: marca.DESCRICAO || null
                },
                { autoCommit: false }
            );

            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_MARCAS 
           WHERE ID_SISTEMA = :idSistema AND CODIGO = :codigo`,
                    { idSistema, codigo: marca.CODIGO },
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
        console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(marcas.length / BATCH_SIZE)}`);
    }

    console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
    return { inseridos, atualizados };
}

/**
 * Sincroniza marcas de uma empresa específica (SINCRONIZAÇÃO TOTAL)
 */
export async function sincronizarMarcasTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀🚀🚀 ================================================`);
        console.log(`🚀 SINCRONIZAÇÃO TOTAL DE MARCAS`);
        console.log(`🚀 ID_SISTEMA: ${idSistema}`);
        console.log(`🚀 Empresa: ${empresaNome}`);
        console.log(`🚀 ================================================\n`);

        const bearerToken = await obterToken(idSistema, true);
        const marcas = await buscarMarcasSankhyaTotal(idSistema, bearerToken);
        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodasComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertMarcas(connection, idSistema, marcas);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_MARCAS',
            STATUS: 'SUCESSO',
            TOTAL_REGISTROS: marcas.length,
            REGISTROS_INSERIDOS: inseridos,
            REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: registrosDeletados,
            DURACAO_MS: duracao,
            DATA_INICIO: dataInicio,
            DATA_FIM: dataFim
        });

        return {
            success: true, idSistema, empresa: empresaNome, totalRegistros: marcas.length,
            registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados,
            dataInicio, dataFim, duracao
        };

    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_MARCAS',
            STATUS: 'FALHA',
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
 * Sincroniza marcas de uma empresa específica (SINCRONIZAÇÃO PARCIAL)
 */
export async function sincronizarMarcasParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀🚀🚀 ================================================`);
        console.log(`🚀 SINCRONIZAÇÃO PARCIAL DE MARCAS`);
        console.log(`🚀 ID_SISTEMA: ${idSistema} - ${empresaNome}`);
        console.log(`🚀 ================================================\n`);

        const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_MARCAS');

        if (!dataUltimaSync) {
            console.log(`⚠️ [Sync] Nenhuma sincronização anterior encontrada. Executando Sincronização Total...`);
            return sincronizarMarcasTotal(idSistema, empresaNome);
        }

        const bearerToken = await obterToken(idSistema, true);
        const marcas = await buscarMarcasSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

        if (marcas.length === 0) {
            console.log(`✅ [Sync] Nenhum novo dado para sincronizar.`);
            const dataFim = new Date();
            return {
                success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
                registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
                dataInicio, dataFim, duracao: dataFim.getTime() - dataInicio.getTime()
            };
        }

        connection = await getOracleConnection();
        const { inseridos, atualizados } = await upsertMarcas(connection, idSistema, marcas);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_MARCAS',
            STATUS: 'SUCESSO',
            TOTAL_REGISTROS: marcas.length,
            REGISTROS_INSERIDOS: inseridos,
            REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: 0,
            DURACAO_MS: duracao,
            DATA_INICIO: dataInicio,
            DATA_FIM: dataFim
        });

        return {
            success: true, idSistema, empresa: empresaNome, totalRegistros: marcas.length,
            registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
            dataInicio, dataFim, duracao
        };

    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_MARCAS',
            STATUS: 'FALHA',
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
 * Sincroniza marcas de uma empresa específica (MANTIDO PARA COMPATIBILIDADE)
 */
export async function sincronizarMarcasPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
    return sincronizarMarcasTotal(idSistema, empresaNome);
}

/**
 * Sincroniza marcas de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
    console.log('🌐 [Sync] Iniciando sincronização de marcas de todas as empresas...');

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
            const resultado = await sincronizarMarcasPorEmpresa(
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
       FROM AS_MARCAS
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
 * Listar marcas sincronizadas
 */
export async function listarMarcas(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE M.ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
                M.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                M.CODIGO,
                M.DESCRICAO,
                M.SANKHYA_ATUAL,
                M.DT_ULT_CARGA
            FROM AS_MARCAS M
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = M.ID_SISTEMA
            ${whereClause}
            ORDER BY M.ID_SISTEMA, M.DESCRICAO
            FETCH FIRST 500 ROWS ONLY`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}
