
const { sincronizarCidadesPorEmpresa } = require('./lib/sync-cidades-service');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function testSync() {
    try {
        console.log("Starting test sync for ID_SISTEMA 1 (DEV)...");
        const result = await sincronizarCidadesPorEmpresa(1, "DEV");
        console.log("Sync Result:", JSON.stringify(result, null, 2));
    } catch (err) {
        console.error("Sync Error:", err);
    }
}

testSync();
