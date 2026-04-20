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

interface BairroSankhya {
    CODBAI: number;
    CODREG: number;
    DESCRICAOCORREIO: string;
    DTALTER: string;
    NOMEBAI: string;
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
 * Busca todos os bairros do Sankhya (SINCRONIZAÇÃO TOTAL)
 */
async function buscarBairrosSankhyaTotal(idSistema: number, bearerToken: string): Promise<BairroSankhya[]> {
    console.log(`🔍 [Sync] Buscando bairros do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allBairros: BairroSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de bairros...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Bairro",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODBAI, CODREG, DESCRICAOCORREIO, DTALTER, NOMEBAI"
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
                console.log(`⚠️ [Sync] Nenhum bairro encontrado na página ${currentPage}`);
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const bairrosPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as BairroSankhya;
            });

            allBairros = allBairros.concat(bairrosPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${bairrosPagina.length} registros (total acumulado: ${allBairros.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (bairrosPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${bairrosPagina.length})`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                console.log(`📊 [Sync] Progresso mantido: ${allBairros.length} registros acumulados`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allBairros.length} bairros recuperados em ${currentPage} páginas`);
    return allBairros;
}

/**
 * Busca bairros do Sankhya alterados após uma data específica (SINCRONIZAÇÃO PARCIAL)
 */
async function buscarBairrosSankhyaParcial(idSistema: number, bearerToken: string, dataUltimaSync: Date): Promise<BairroSankhya[]> {
    const dia = dataUltimaSync.getDate().toString().padStart(2, '0');
    const mes = (dataUltimaSync.getMonth() + 1).toString().padStart(2, '0');
    const ano = dataUltimaSync.getFullYear();
    const hora = dataUltimaSync.getHours().toString().padStart(2, '0');
    const min = dataUltimaSync.getMinutes().toString().padStart(2, '0');
    const seg = dataUltimaSync.getSeconds().toString().padStart(2, '0');
    const dataFormatada = `${dia}/${mes}/${ano} ${hora}:${min}:${seg}`;

    console.log(`🔍 [Sync] [Parcial] Buscando bairros alterados desde ${dataFormatada}`);

    let allBairros: BairroSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;
    let currentToken = bearerToken;

    while (hasMoreData) {
        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Bairro",
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
                            "list": "CODBAI, CODREG, DESCRICAOCORREIO, DTALTER, NOMEBAI"
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

            const bairrosPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) cleanObject[fieldName] = rawEntity[fieldKey].$;
                }
                return cleanObject as BairroSankhya;
            });

            allBairros = allBairros.concat(bairrosPagina);
            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (bairrosPagina.length === 0 || !hasMoreResult) {
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
    return allBairros;
}

/**
 * Sincronização Parcial de bairros
 */
export async function sincronizarBairrosParcial(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀 [Sync] [Parcial] BAIRROS: ${empresaNome}`);
        const dataUltimaSync = await buscarDataUltimaSincronizacao(idSistema, 'AS_BAIRROS');

        if (!dataUltimaSync) return sincronizarBairrosTotal(idSistema, empresaNome);

        const bearerToken = await obterToken(idSistema, true);
        const bairros = await buscarBairrosSankhyaParcial(idSistema, bearerToken, dataUltimaSync);

        if (bairros.length === 0) {
            return {
                success: true, idSistema, empresa: empresaNome, totalRegistros: 0,
                registrosInseridos: 0, registrosAtualizados: 0, registrosDeletados: 0,
                dataInicio, dataFim: new Date(), duracao: 0
            };
        }

        connection = await getOracleConnection();
        const { inseridos, atualizados } = await upsertBairros(connection, idSistema, bairros);
        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_BAIRROS', STATUS: 'SUCESSO',
            TOTAL_REGISTROS: bairros.length, REGISTROS_INSERIDOS: inseridos, REGISTROS_ATUALIZADOS: atualizados,
            REGISTROS_DELETADOS: 0, DURACAO_MS: duracao, DATA_INICIO: dataInicio, DATA_FIM: dataFim
        });

        return {
            success: true, idSistema, empresa: empresaNome, totalRegistros: bairros.length,
            registrosInseridos: inseridos, registrosAtualizados: atualizados, registrosDeletados: 0,
            dataInicio, dataFim, duracao
        };
    } catch (error: any) {
        if (connection) await connection.rollback().catch(() => { });
        const dataFim = new Date();
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema, EMPRESA: empresaNome, TABELA: 'AS_BAIRROS', STATUS: 'FALHA',
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
 * Converter data do formato Sankhya para Date do Oracle
 */
function parseDataSankhya(dataStr: string | undefined): Date | null {
    if (!dataStr) return null;

    try {
        const partes = dataStr.trim().split(' ');
        const dataParte = partes[0];
        const horaParte = partes[1] || '00:00:00';

        const [dia, mes, ano] = dataParte.split('/'); // Assumindo DD/MM/YYYY

        if (!dia || !mes || !ano) {
            // Tentar formato YYYY-MM-DD se o split falhar ou se vier diferente
            const isoCheck = new Date(dataStr);
            if (!isNaN(isoCheck.getTime())) return isoCheck;
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
 * Executa o soft delete (marca como não atual) todos os bairros do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_BAIRROS 
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
 * Executa UPSERT de bairros usando MERGE
 */
async function upsertBairros(
    connection: oracledb.Connection,
    idSistema: number,
    bairros: BairroSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < bairros.length; i += BATCH_SIZE) {
        const batch = bairros.slice(i, i + BATCH_SIZE);

        for (const bairro of batch) {
            const dtAlter = parseDataSankhya(bairro.DTALTER);

            const result = await connection.execute(
                `MERGE INTO AS_BAIRROS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codBai AS CODBAI FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODBAI = src.CODBAI)
         WHEN MATCHED THEN
           UPDATE SET
             CODREG = :codReg,
             DESCRICAOCORREIO = :descricaoCorreio,
             DTALTER = :dtAlter,
             NOMEBAI = :nomeBai,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODBAI, CODREG, DESCRICAOCORREIO, DTALTER, NOMEBAI,
             SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codBai, :codReg, :descricaoCorreio, :dtAlter, :nomeBai,
             'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codBai: bairro.CODBAI || null,
                    codReg: bairro.CODREG || null,
                    descricaoCorreio: bairro.DESCRICAOCORREIO || null,
                    dtAlter: dtAlter,
                    nomeBai: bairro.NOMEBAI || null
                },
                { autoCommit: false }
            );

            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_BAIRROS 
            WHERE ID_SISTEMA = :idSistema AND CODBAI = :codBai`,
                    { idSistema, codBai: bairro.CODBAI },
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
        console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(bairros.length / BATCH_SIZE)}`);
    }

    console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
    return { inseridos, atualizados };
}

/**
 * Sincronização Total de bairros
 */
export async function sincronizarBairrosTotal(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`\n🚀🚀🚀 ================================================`);
        console.log(`🚀 SINCRONIZAÇÃO DE BAIRROS`);
        console.log(`🚀 ID_SISTEMA: ${idSistema}`);
        console.log(`🚀 Empresa: ${empresaNome}`);
        console.log(`🚀 ================================================\n`);

        // SEMPRE forçar renovação do token
        console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
        let bearerToken = await obterToken(idSistema, true);
        const bairros = await buscarBairrosSankhyaTotal(idSistema, bearerToken);
        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertBairros(connection, idSistema, bairros);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
        console.log(`📊 [Sync] Resumo: ${bairros.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
        console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

        // Salvar log de sucesso
        await salvarLogSincronizacao({
            ID_SISTEMA: idSistema,
            EMPRESA: empresaNome,
            TABELA: 'AS_BAIRROS',
            STATUS: 'SUCESSO',
            TOTAL_REGISTROS: bairros.length,
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
            totalRegistros: bairros.length,
            registrosInseridos: inseridos,
            registrosAtualizados: atualizados,
            registrosDeletados,
            dataInicio,
            dataFim,
            duracao
        };

    } catch (error: any) {
        console.error(`❌ [Sync] Erro ao sincronizar bairros para ${empresaNome}:`, error);

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
            TABELA: 'AS_BAIRROS',
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
 * Sincroniza bairros de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
    console.log('🌐 [Sync] Iniciando sincronização de bairros de todas as empresas...');

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
            const resultado = await sincronizarBairrosTotal(
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
       FROM AS_BAIRROS
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
 * Listar bairros sincronizados (Para o frontend)
 */
export async function listarBairros(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE B.ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
                B.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                B.CODBAI,
                B.NOMEBAI,
                B.CODREG,
                B.DESCRICAOCORREIO,
                B.SANKHYA_ATUAL,
                B.DT_ULT_CARGA
            FROM AS_BAIRROS B
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = B.ID_SISTEMA
            ${whereClause}
            ORDER BY B.ID_SISTEMA, B.NOMEBAI`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}
