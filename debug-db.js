
const oracledb = require('oracledb');
const path = require('path');
const fs = require('fs');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function checkDatabase() {
    let connection;
    try {
        console.log("Connecting to Oracle...");
        console.log("User:", process.env.ORACLE_USER);
        console.log("Connect String:", process.env.ORACLE_CONNECT_STRING);

        connection = await oracledb.getConnection({
            user: process.env.ORACLE_USER,
            password: process.env.ORACLE_PASSWORD,
            connectString: process.env.ORACLE_CONNECT_STRING
        });

        const tablesToCheck = ['AS_CIDADES', 'SYNC_LOGS', 'AD_CONTRATOS'];

        for (const tableName of tablesToCheck) {
            console.log(`\n--- Checking table ${tableName} ---`);
            try {
                const countResult = await connection.execute(`SELECT COUNT(*) as CNT FROM ${tableName}`);
                console.log(`Table ${tableName} exists. Count: ${countResult.rows[0][0]}`);

                if (tableName === 'AS_CIDADES') {
                    const stats = await connection.execute(
                        `SELECT ID_SISTEMA, SANKHYA_ATUAL, COUNT(*) as CNT FROM AS_CIDADES GROUP BY ID_SISTEMA, SANKHYA_ATUAL`
                    );
                    console.table(stats.rows);
                }

                if (tableName === 'SYNC_LOGS') {
                    const logs = await connection.execute(
                        `SELECT * FROM (SELECT TABELA, STATUS, DATA_FIM, MENSAGEM_ERRO FROM SYNC_LOGS ORDER BY DATA_FIM DESC) WHERE ROWNUM <= 5`
                    );
                    console.table(logs.rows);
                }

                if (tableName === 'AD_CONTRATOS') {
                    const contracts = await connection.execute(
                        `SELECT ID_EMPRESA, EMPRESA, ATIVO FROM AD_CONTRATOS WHERE ATIVO = 'S'`
                    );
                    console.table(contracts.rows);
                }
            } catch (e) {
                console.log(`Error checking table ${tableName}: ${e.message}`);
            }
        }

    } catch (err) {
        console.error("Database connection error:", err.message);
    } finally {
        if (connection) {
            await connection.close();
        }
    }
}

checkDatabase();
