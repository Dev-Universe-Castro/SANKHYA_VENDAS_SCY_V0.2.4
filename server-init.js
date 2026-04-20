
/**
 * Script de inicialização do servidor
 * A inicialização do token agora é feita via instrumentation.ts
 */

console.log('🔥 [SERVER-INIT] Carregando variáveis de ambiente...');

// Carregar variáveis de ambiente
require('dotenv').config({ path: './config.env.local' });

console.log('✅ [SERVER-INIT] Variáveis carregadas. Next.js iniciará em seguida...');
