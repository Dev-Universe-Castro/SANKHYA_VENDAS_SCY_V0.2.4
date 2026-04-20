
const oracledb = require('oracledb');

async function check() {
    let conn;
    try {
        conn = await oracledb.getConnection({
            user: "SYSTEM",
            password: "Castro135!",
            connectString: "crescimentoerp.nuvemdatacom.com.br:9568/FREEPDB1"
        });
        const result = await conn.execute(
            `SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME LIKE 'AD_%' OR TABLE_NAME LIKE 'AS_%'`
        );
        console.log(JSON.stringify(result.rows, null, 2));
    } catch (e) {
        console.error(e);
    } finally {
        if (conn) await conn.close();
    }
}
check();
