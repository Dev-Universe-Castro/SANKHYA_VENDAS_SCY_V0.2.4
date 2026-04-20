// Carregar variáveis de ambiente do arquivo local (se existir)
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env.local');

if (fs.existsSync(envPath)) {
  require('dotenv').config({ path: envPath });
  console.log('✅ Variáveis carregadas de .env.local');
} else {
  console.log('⚠️ Arquivo .env.local não encontrado, usando variáveis do sistema');
}

// Carrega as variáveis do arquivo local para memória
require('dotenv').config({ path: path.join(__dirname, 'config.env.local') });

// Validar variáveis críticas
const requiredVars = [
  'SANKHYA_TOKEN',
  'SANKHYA_APPKEY',
  'SANKHYA_USERNAME',
  'SANKHYA_PASSWORD'
];

const missingVars = requiredVars.filter(v => !process.env[v]);
if (missingVars.length > 0) {
  console.error('❌ ERRO: Variáveis de ambiente obrigatórias não encontradas:', missingVars);
  console.error('Configure as variáveis no sistema ou crie o arquivo config.env.local');
  process.exit(1);
}

module.exports = {
  apps : [{
    name: "SankhyaSincronizadorVendas",
    // Aponta para o servidor otimizado (Standalone)
    script: ".next/standalone/server.js",
    instances: 1,
    exec_mode: "fork",
    watch: false,
    env: {
      NODE_ENV: "production",
      PORT: 4000,
      HOSTNAME: "0.0.0.0",

      // === CORREÇÃO CRÍTICA (Conexão Local) ===
      // Define explicitamente localhost para evitar sair pelo firewall e ser bloqueado
      ORACLE_CONNECT_STRING: "localhost:1521/FREEPDB1",

      // Credenciais do Banco
      ORACLE_USER: process.env.ORACLE_USER || "SYSTEM",
      ORACLE_PASSWORD: process.env.ORACLE_PASSWORD, // Pega do config.env.local

      // Variáveis da API Sankhya e IA
      SANKHYA_TOKEN: process.env.SANKHYA_TOKEN,
      SANKHYA_APPKEY: process.env.SANKHYA_APPKEY,
      SANKHYA_USERNAME: process.env.SANKHYA_USERNAME,
      SANKHYA_PASSWORD: process.env.SANKHYA_PASSWORD,
      GEMINI_API_KEY: process.env.GEMINI_API_KEY,

      // URL do Site
      NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL
    }
  }]
};
