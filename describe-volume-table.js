
const oracledb = require('oracledb');
try {
    require('dotenv').config({ path: 'config.env.local' });
} catch (e) {}

async function describeTable() {
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
            `SELECT column_name, data_type, data_length, nullable 
             FROM user_tab_columns 
             WHERE table_name = 'AS_VOLUME_ALTERNATIVO' 
             ORDER BY column_id`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        console.log('Table structure:');
        result.rows.forEach(row => {
            console.log(` - ${row.COLUMN_NAME}: ${row.DATA_TYPE}(${row.DATA_LENGTH}) ${row.NULLABLE === 'Y' ? 'NULL' : 'NOT NULL'}`);
        });

    } catch (err) {
        console.error('Error:', err);
    } finally {
        if (connection) {
            try { await connection.close(); } catch (err) {}
        }
    }
}

describeTable();
