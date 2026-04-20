
const oracledb = require('oracledb');
try {
    require('dotenv').config({ path: 'config.env.local' });
} catch (e) {}

async function listTables() {
    let connection;
    try {
        const config = {
            user: process.env.ORACLE_USER || 'SYSTEM',
            password: process.env.ORACLE_PASSWORD || 'Castro135!',
            connectString: process.env.ORACLE_CONNECT_STRING || 'crescimentoerp.nuvemdatacom.com.br:9568/FREEPDB1'
        };

        connection = await oracledb.getConnection(config);
        console.log('Connected!');

        const result = await connection.execute(
            `SELECT table_name FROM user_tables WHERE table_name LIKE 'AS_%' ORDER BY table_name`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        console.log('Tables found:');
        result.rows.forEach(row => console.log(` - ${row.TABLE_NAME}`));

    } catch (err) {
        console.error('Error:', err);
    } finally {
        if (connection) {
            try { await connection.close(); } catch (err) {}
        }
    }
}

listTables();
