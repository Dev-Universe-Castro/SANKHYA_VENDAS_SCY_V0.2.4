
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
            `SELECT ID_EMPRESA, EMPRESA, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, IS_SANDBOX, AUTH_TYPE, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_X_TOKEN
             FROM AD_CONTRATOS WHERE ID_EMPRESA = 61`
        );
        console.log(JSON.stringify(result.rows, null, 2));
    } catch (e) {
        console.error(e);
    } finally {
        if (conn) await conn.close();
    }
}
check();
