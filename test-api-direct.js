
const axios = require('axios');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

async function testSankhyaApi() {
    try {
        console.log("Starting manual login for Contract 1...");
        const loginHeaders = {
            'token': process.env.SANKHYA_TOKEN,
            'appkey': process.env.SANKHYA_APPKEY,
            'username': process.env.SANKHYA_USERNAME,
            'password': process.env.SANKHYA_PASSWORD
        };
        const loginUrl = "https://api.sandbox.sankhya.com.br/login";
        const loginResponse = await axios.post(loginUrl, {}, { headers: loginHeaders });
        const token = loginResponse.data.bearerToken || loginResponse.data.token;
        console.log("Token obtained.");

        const testFields = ["UF"];
        const baseUrl = "https://api.sandbox.sankhya.com.br";
        const url = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

        for (const field of testFields) {
            console.log(`Testing field: ${field}...`);
            const payload = {
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "Produto",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "entity": {
                            "fieldset": {
                                "list": `CODPROD, CODVOL`
                            }
                        }
                    }
                }
            };
            const response = await axios.post(url, payload, {
                headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
            });

            if (response.data.status === '1') {
                console.log(`✅ Field ${field} is OK.`);
                console.log("Sample:", JSON.stringify(response.data.responseBody.entities.entity[0], null, 2));
            } else {
                console.log(`❌ Field ${field} failed: ${response.data.statusMessage}`);
            }
        }

    } catch (err) {
        console.error("API Error:", err.message);
    }
}

testSankhyaApi();
