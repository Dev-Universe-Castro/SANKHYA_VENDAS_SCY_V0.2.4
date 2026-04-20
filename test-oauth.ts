import { buscarContratoPorId } from './lib/oracle-service';
import axios from 'axios';
import * as dotenv from 'dotenv';
dotenv.config({ path: 'config.env.local' });
const oracledb = require('oracledb');

async function test() {
    try {
        const contrato = await buscarContratoPorId(41);

        const baseUrl = "https://api.sandbox.sankhya.com.br";
        const oauthUrl = `${baseUrl}/authenticate`;

        // Teste 1: URLSearchParams with Trim
        const clientId = contrato.OAUTH_CLIENT_ID?.trim();
        const clientSecret = contrato.OAUTH_CLIENT_SECRET?.trim();
        const xToken = contrato.OAUTH_X_TOKEN?.trim();

        console.log(`CLIENT_ID [${clientId}]`);
        console.log(`CLIENT_SECRET [${clientSecret.substring(0, 5)}...]`);
        console.log(`X-TOKEN [${xToken}]`);

        const params = new URLSearchParams();
        params.append('grant_type', 'client_credentials');
        params.append('client_id', clientId);
        params.append('client_secret', clientSecret);

        const config = {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Token': xToken
            }
        };

        console.log("Requisitando Token de:", oauthUrl);
        const resposta = await axios.post(oauthUrl, params, config);
        console.log("SUCESSO:", resposta.data);
    } catch (e: any) {
        if (e.response) {
            console.log("ERRO HTTP:", e.response.status);
            console.log("ERRO DATA:", JSON.stringify(e.response.data, null, 2));
        } else {
            console.log("ERRO MESSAGE:", e.message);
        }
    } finally {
        try {
            await oracledb.getPool().close(10);
            console.log("Pool fechado.");
        } catch (err) { }
        process.exit(0);
    }
}
test();
