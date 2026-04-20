
const oracledb = require('oracledb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function checkContracts() {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONNECT_STRING
        });

        const result = await connection.execute(
            `SELECT ID_EMPRESA, EMPRESA, ATIVO, AUTH_TYPE, IS_SANDBOX FROM AD_CONTRATOS WHERE ATIVO = 'S'`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        console.table(result.rows);

    } catch (err) {
        console.error("Error:", err.message);
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

checkContracts();
