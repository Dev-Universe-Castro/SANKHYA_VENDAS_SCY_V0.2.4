
const oracledb = require('oracledb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function listTables() {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONNECT_STRING
        });

        const result = await connection.execute(
            `SELECT table_name FROM user_tables WHERE table_name LIKE 'AS_%' OR table_name LIKE 'SYNC_%' OR table_name = 'AD_CONTRATOS'`
        );
        console.log("Tables found:");
        console.table(result.rows);

    } catch (err) {
        console.error("Error:", err.message);
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

listTables();
