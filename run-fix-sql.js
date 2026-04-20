
const oracledb = require('oracledb');
const fs = require('fs');
const path = require('path');

// Carregar configurações do env se existir
try {
    require('dotenv').config({ path: 'config.env.local' });
} catch (e) {}

async function runFix() {
    let connection;
    try {
        const config = {
            user: process.env.ORACLE_USER || 'SYSTEM',
            password: process.env.ORACLE_PASSWORD || 'oracle',
            connectString: process.env.ORACLE_CONNECT_STRING || 'localhost:1521/XEPDB1'
        };

        console.log('Connecting to Oracle...');
        connection = await oracledb.getConnection(config);
        console.log('Connected!');

        const sqlFile = path.join(__dirname, 'fix_missing_columns.sql');
        const sqlContent = fs.readFileSync(sqlFile, 'utf8');

        // Split by '/' at the beginning of a line
        const commands = sqlContent.split(/\n\s*\/\s*\n/);

        for (let cmd of commands) {
            cmd = cmd.trim();
            if (!cmd) continue;

            console.log(`Executing:\n${cmd.substring(0, 100)}...`);
            try {
                // Remove comments for execution if they are at the start
                const cleanCmd = cmd.replace(/^--.*$/gm, '').trim();
                if (!cleanCmd) continue;
                
                await connection.execute(cleanCmd);
                console.log('✅ Command executed successfully');
            } catch (err) {
                if (err.message.includes('ORA-01430') || err.message.includes('already exists')) {
                    console.log('ℹ️ Column already exists, skipping.');
                } else {
                    console.error('❌ Error executing command:', err.message);
                }
            }
        }

        await connection.commit();
        console.log('Done!');

    } catch (err) {
        console.error('Fatal error:', err);
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error(err);
            }
        }
    }
}

runFix();
