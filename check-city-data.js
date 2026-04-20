
const oracledb = require('oracledb');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function checkData() {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONNECT_STRING
        });

        const result = await connection.execute(
            `SELECT ID_SISTEMA, COUNT(*) as CNT FROM AS_CIDADES GROUP BY ID_SISTEMA`
        );
        console.table(result.rows);

        const logs = await connection.execute(
            `SELECT * FROM (
                SELECT ID_SISTEMA, TABELA, STATUS, TOTAL_REGISTROS, REGISTROS_INSERIDOS, DATA_FIM, MENSAGEM_ERRO
                FROM AS_SYNC_LOGS 
                WHERE TABELA = 'AS_CIDADES'
                ORDER BY DATA_FIM DESC
            ) WHERE ROWNUM <= 10`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        console.table(logs.rows);

    } catch (err) {
        console.error("Error:", err.message);
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

checkData();
