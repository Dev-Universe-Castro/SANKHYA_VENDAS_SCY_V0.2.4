
const oracledb = require('oracledb');
try {
    require('dotenv').config({ path: 'config.env.local' });
} catch (e) {}

async function check() {
    let connection;
    try {
        const config = {
            user: process.env.ORACLE_USER || 'SYSTEM',
            password: process.env.ORACLE_PASSWORD || 'Castro135!',
            connectString: process.env.ORACLE_CONNECT_STRING || 'crescimentoerp.nuvemdatacom.com.br:9568/FREEPDB1'
        };

        connection = await oracledb.getConnection(config);
        const result = await connection.execute(
            `SELECT * FROM AS_SYNC_LOGS WHERE TABELA = 'AS_PRODUTOS' ORDER BY DATA_FIM DESC FETCH FIRST 1 ROWS ONLY`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        console.log('Last Sync Log:', JSON.stringify(result.rows[0], null, 2));

    } catch (err) {
        console.error('Error:', err);
    } finally {
        if (connection) {
            try { await connection.close(); } catch (err) {}
        }
    }
}

check();
