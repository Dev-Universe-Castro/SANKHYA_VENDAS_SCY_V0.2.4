
const { sincronizarProdutosTotal } = require('./lib/sync-produtos-service');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function debugSync() {
    try {
        console.log("Starting debug sync for Contract 41...");
        const result = await sincronizarProdutosTotal(41, "COPINI");
        console.log("Result:", JSON.stringify(result, null, 2));
    } catch (err) {
        console.error("DEBUG ERROR:", err);
    }
}

debugSync();
