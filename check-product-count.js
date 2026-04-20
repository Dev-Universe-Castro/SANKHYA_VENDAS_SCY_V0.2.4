
const oracledb = require('oracledb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function checkProducts() {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONNECT_STRING
        });

        const result = await connection.execute(
            `SELECT ID_SISTEMA, COUNT(*) as CNT FROM AS_PRODUTOS GROUP BY ID_SISTEMA`
        );
        console.log("Product counts:");
        console.table(result.rows);

    } catch (err) {
        console.error("Error:", err.message);
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

checkProducts();
