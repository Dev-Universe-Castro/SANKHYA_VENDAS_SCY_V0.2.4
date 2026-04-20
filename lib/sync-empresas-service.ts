import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';

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

interface EmpresaSankhya {
    CODEMP: number;
    NOMEFANTASIA: string;
    RAZAOSOCIAL: string;
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
 * Busca todas as empresas do Sankhya para uma empresa específica
 */
async function buscarEmpresasSankhya(idSistema: number, bearerToken: string): Promise<EmpresaSankhya[]> {
    console.log(`🔍 [Sync] Buscando empresas do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allEmpresas: EmpresaSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de empresas...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Empresa",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODEMP, NOMEFANTASIA, RAZAOSOCIAL"
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
                console.log(`⚠️ [Sync] Nenhuma empresa encontrada na página ${currentPage}`);
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            // Corrige para array se vier objeto único
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const empresasPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as EmpresaSankhya;
            });

            allEmpresas = allEmpresas.concat(empresasPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${empresasPagina.length} registros (total acumulado: ${allEmpresas.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (empresasPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${empresasPagina.length})`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                console.log(`📊 [Sync] Progresso mantido: ${allEmpresas.length} registros acumulados`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allEmpresas.length} empresas recuperadas em ${currentPage} páginas`);
    return allEmpresas;
}

/**
 * Executa o soft delete (marca como não atual) todas as empresas do sistema
 */
async function marcarTodasComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_EMPRESAS 
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
 * Executa UPSERT de empresas usando MERGE
 */
async function upsertEmpresas(
    connection: oracledb.Connection,
    idSistema: number,
    empresas: EmpresaSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < empresas.length; i += BATCH_SIZE) {
        const batch = empresas.slice(i, i + BATCH_SIZE);

        for (const empresa of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_EMPRESAS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codEmp AS CODEMP FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODEMP = src.CODEMP)
         WHEN MATCHED THEN
           UPDATE SET
             NOMEFANTASIA = :nomeFantasia,
             RAZAOSOCIAL = :razaoSocial,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODEMP, NOMEFANTASIA, RAZAOSOCIAL,
             SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codEmp, :nomeFantasia, :razaoSocial,
             'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codEmp: empresa.CODEMP || null,
                    nomeFantasia: empresa.NOMEFANTASIA || null,
                    razaoSocial: empresa.RAZAOSOCIAL || null
                },
                { autoCommit: false }
            );

            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_EMPRESAS 
            WHERE ID_SISTEMA = :idSistema AND CODEMP = :codEmp`,
                    { idSistema, codEmp: empresa.CODEMP },
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
        console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(empresas.length / BATCH_SIZE)}`);
    }

    console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
    return { inseridos, atualizados };
}

/**
 * Sincroniza empresas de uma empresa específica
 */
export async function sincronizarEmpresasPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀🚀🚀 ================================================`);
        console.log(`🚀 SINCRONIZAÇÃO DE EMPRESAS`);
        console.log(`🚀 ID_SISTEMA: ${idSistema}`);
        console.log(`🚀 Empresa: ${empresaNome}`);
        console.log(`🚀 ================================================\n`);

        // SEMPRE forçar renovação do token
        console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
        let bearerToken = await obterToken(idSistema, true);
        const empresas = await buscarEmpresasSankhya(idSistema, bearerToken);
        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodasComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertEmpresas(connection, idSistema, empresas);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
        console.log(`📊 [Sync] Resumo: ${empresas.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
        console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

        // Salvar log de sucesso
        try {
            const { salvarLogSincronizacao } = await import('./sync-logs-service');
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_EMPRESAS',
                STATUS: 'SUCESSO',
                TOTAL_REGISTROS: empresas.length,
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
            totalRegistros: empresas.length,
            registrosInseridos: inseridos,
            registrosAtualizados: atualizados,
            registrosDeletados,
            dataInicio,
            dataFim,
            duracao
        };

    } catch (error: any) {
        console.error(`❌ [Sync] Erro ao sincronizar empresas para ${empresaNome}:`, error);

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
            const { salvarLogSincronizacao } = await import('./sync-logs-service');
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_EMPRESAS',
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
 * Sincroniza empresas de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
    console.log('🌐 [Sync] Iniciando sincronização de empresas de todas as empresas...');

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
            const resultado = await sincronizarEmpresasPorEmpresa(
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
       FROM AS_EMPRESAS
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
 * Listar empresas sincronizadas
 */
export async function listarEmpresas(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
                E.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                E.CODEMP,
                E.NOMEFANTASIA,
                E.RAZAOSOCIAL,
                E.SANKHYA_ATUAL,
                E.DT_ULT_CARGA
            FROM AS_EMPRESAS E
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = E.ID_SISTEMA
            ${whereClause}
            ORDER BY E.ID_SISTEMA, E.CODEMP`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}
