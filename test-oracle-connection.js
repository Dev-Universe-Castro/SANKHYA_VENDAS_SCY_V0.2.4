
const oracledb = require('oracledb');
require('dotenv').config({ path: '.env.local' });

async function testConnection() {
  console.log('🔍 Testando conexão Oracle...\n');
  
  const config = {
    user: process.env.ORACLE_USER,
    password: process.env.ORACLE_PASSWORD,
    connectString: process.env.ORACLE_CONNECT_STRING
  };
  
  console.log('📋 Configurações carregadas:');
  console.log('   User:', config.user);
  console.log('   Connect String:', config.connectString);
  console.log('   Password length:', config.password?.length);
  console.log('');
  
  let connection;
  
  try {
    console.log('🔌 Tentando conectar...');
    connection = await oracledb.getConnection(config);
    console.log('✅ Conexão estabelecida com sucesso!\n');
    
    // Testar uma query simples
    console.log('📊 Executando query de teste...');
    const result = await connection.execute('SELECT SYSDATE FROM DUAL');
    console.log('✅ Query executada:', result.rows);
    console.log('');
    
    // Verificar se a tabela existe
    console.log('🔍 Verificando tabela AD_CONTRATOS...');
    try {
      const tableCheck = await connection.execute(
        `SELECT COUNT(*) as CNT FROM USER_TABLES WHERE TABLE_NAME = 'AD_CONTRATOS'`
      );
      console.log('✅ Tabela existe:', tableCheck.rows[0][0] > 0);
    } catch (err) {
      console.log('❌ Erro ao verificar tabela:', err.message);
    }
    
  } catch (error) {
    console.error('❌ Erro na conexão:');
    console.error('   Mensagem:', error.message);
    console.error('   Código:', error.errorNum);
    if (error.offset) {
      console.error('   Offset:', error.offset);
    }
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log('\n✅ Conexão fechada');
      } catch (err) {
        console.error('⚠️ Erro ao fechar conexão:', err.message);
      }
    }
  }
}

testConnection();
