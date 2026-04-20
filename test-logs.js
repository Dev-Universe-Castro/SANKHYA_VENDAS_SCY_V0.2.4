const oracledb = require('oracledb');
require('dotenv').config({ path: 'config.env.local' });

async function check() {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONN_STRING
        });

        const result = await connection.execute(
            `SELECT ID_LOG, TABELA, STATUS, DATA_INICIO, DATA_CRIACAO FROM AS_SYNC_LOGS WHERE ID_SISTEMA = 61 AND TABELA = 'AS_GRUPOS_PRODUTOS' ORDER BY DATA_CRIACAO DESC FETCH FIRST 5 ROWS ONLY`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        console.log("LOGS PARA 61 GRUPOS PRODUTOS:");
        console.table(result.rows);

        // Check what buscarDataUltimaSincronizacao would return
        const resultUltima = await connection.execute(
            `SELECT MAX(DATA_INICIO) as ULTIMA_DATA
       FROM AS_SYNC_LOGS
       WHERE ID_SISTEMA = 61
         AND TABELA = 'AS_GRUPOS_PRODUTOS'
         AND STATUS = 'SUCESSO'`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        console.log("MAX(DATA_INICIO):", resultUltima.rows);

    } catch (err) {
        console.error(err);
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (err) { }
        }
        process.exit(0);
    }
}
check();
