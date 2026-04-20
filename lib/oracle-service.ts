import oracledb from 'oracledb';

interface OracleConfig {
  user: string;
  password: string;
  connectString: string;
}

let pool: oracledb.Pool | null = null;

export async function initOraclePool(config: OracleConfig) {
  if (pool) {
    console.log('♻️ Reutilizando pool Oracle existente');
    return pool;
  }

  console.log('🔧 Criando novo pool Oracle com configurações:', {
    user: config.user,
    connectString: config.connectString,
    hasPassword: !!config.password,
    passwordLength: config.password?.length
  });

  try {
    pool = await oracledb.createPool({
      user: config.user,
      password: config.password,
      connectString: config.connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 60,
      connectionTimeout: 60000
    });

    console.log('✅ Pool Oracle criado com sucesso');

    // Testar conexão
    const testConn = await pool.getConnection();
    console.log('✅ Conexão de teste bem-sucedida');
    await testConn.close();

    return pool;
  } catch (error: any) {
    console.error('❌ Erro ao criar pool Oracle:', {
      message: error.message,
      code: error.errorNum,
      offset: error.offset
    });
    throw error;
  }
}

export async function getOracleConnection() {
  if (!pool) {
    const config: OracleConfig = {
      user: process.env.ORACLE_USER || '',
      password: process.env.ORACLE_PASSWORD || '',
      connectString: process.env.ORACLE_CONNECT_STRING || ''
    };

    console.log('🔑 Credenciais Oracle carregadas:', {
      user: config.user,
      connectString: config.connectString,
      hasPassword: !!config.password
    });

    await initOraclePool(config);
  }

  return pool!.getConnection();
}

export async function closeOraclePool() {
  if (pool) {
    await pool.close(10);
    pool = null;
    console.log('✅ Pool Oracle fechado');
  }
}

export interface Contrato {
  ID_EMPRESA: number;
  EMPRESA: string;
  CNPJ: string;
  SANKHYA_TOKEN: string;
  SANKHYA_APPKEY: string;
  SANKHYA_USERNAME: string;
  SANKHYA_PASSWORD: string;
  OAUTH_CLIENT_ID?: string;
  OAUTH_CLIENT_SECRET?: string;
  OAUTH_X_TOKEN?: string;
  AUTH_TYPE?: 'LEGACY' | 'OAUTH2';
  GEMINI_API_KEY: string;
  AI_PROVEDOR?: string;
  AI_MODELO?: string;
  AI_CREDENTIAL?: string;
  ATIVO: boolean;
  IS_SANDBOX?: boolean;
  SYNC_ATIVO?: boolean;
  SYNC_INTERVALO_MINUTOS?: number;
  ULTIMA_SINCRONIZACAO?: Date;
  PROXIMA_SINCRONIZACAO?: Date;
  DATA_CRIACAO?: Date;
  DATA_ATUALIZACAO?: Date;
  LICENCAS?: string;
}

export async function listarContratos(): Promise<Contrato[]> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `SELECT ID_EMPRESA, EMPRESA, CNPJ, 
              SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD,
              OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_X_TOKEN, AUTH_TYPE,
              GEMINI_API_KEY, AI_PROVEDOR, AI_MODELO, AI_CREDENTIAL,
              ATIVO, IS_SANDBOX, SYNC_ATIVO, SYNC_INTERVALO_MINUTOS,
              ULTIMA_SINCRONIZACAO, PROXIMA_SINCRONIZACAO, DATA_CRIACAO, DATA_ATUALIZACAO,
              LICENCAS
       FROM AD_CONTRATOS 
       ORDER BY EMPRESA ASC`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const contratos = (result.rows as any[]).map(row => {
      const isSandbox = row.IS_SANDBOX === 'S';
      const syncAtivo = row.SYNC_ATIVO === 'S';
      console.log(`🔍 [Oracle] Contrato ${row.ID_EMPRESA} - IS_SANDBOX no BD: '${row.IS_SANDBOX}', convertido para boolean: ${isSandbox}`);
      return {
        ID_EMPRESA: row.ID_EMPRESA,
        EMPRESA: row.EMPRESA,
        CNPJ: row.CNPJ,
        SANKHYA_TOKEN: row.SANKHYA_TOKEN || '',
        SANKHYA_APPKEY: row.SANKHYA_APPKEY || '',
        SANKHYA_USERNAME: row.SANKHYA_USERNAME || '',
        SANKHYA_PASSWORD: row.SANKHYA_PASSWORD || '',
        OAUTH_CLIENT_ID: row.OAUTH_CLIENT_ID || '',
        OAUTH_CLIENT_SECRET: row.OAUTH_CLIENT_SECRET || '',
        OAUTH_X_TOKEN: row.OAUTH_X_TOKEN || '',
        AUTH_TYPE: row.AUTH_TYPE || 'LEGACY',
        GEMINI_API_KEY: row.GEMINI_API_KEY || '',
        AI_PROVEDOR: row.AI_PROVEDOR || 'Gemini',
        AI_MODELO: row.AI_MODELO || 'gemini-2.0-flash',
        AI_CREDENTIAL: row.AI_CREDENTIAL || '',
        ATIVO: row.ATIVO === 'S',
        IS_SANDBOX: isSandbox,
        SYNC_ATIVO: syncAtivo,
        SYNC_INTERVALO_MINUTOS: row.SYNC_INTERVALO_MINUTOS || 120,
        ULTIMA_SINCRONIZACAO: row.ULTIMA_SINCRONIZACAO,
        PROXIMA_SINCRONIZACAO: row.PROXIMA_SINCRONIZACAO,
        DATA_CRIACAO: row.DATA_CRIACAO,
        DATA_ATUALIZACAO: row.DATA_ATUALIZACAO,
        LICENCAS: row.LICENCAS
      };
    });

    return contratos;
  } finally {
    await connection.close();
  }
}

export async function criarContrato(contrato: Omit<Contrato, 'ID_EMPRESA'>): Promise<number> {
  console.log('📝 Iniciando criação de contrato:', { empresa: contrato.EMPRESA, cnpj: contrato.CNPJ });

  let connection;
  try {
    connection = await getOracleConnection();
    console.log('✅ Conexão Oracle obtida');

    const result = await connection.execute(
      `INSERT INTO AD_CONTRATOS 
        (EMPRESA, CNPJ, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, GEMINI_API_KEY, AI_PROVEDOR, AI_MODELO, AI_CREDENTIAL, ATIVO, LICENCAS)
      VALUES 
        (:empresa, :cnpj, :token, :appkey, :username, :password, :gemini, :aiProvedor, :aiModelo, :aiCredential, :ativo, :licencas)`,
      {
        empresa: contrato.EMPRESA,
        cnpj: contrato.CNPJ,
        token: contrato.SANKHYA_TOKEN || null,
        appkey: contrato.SANKHYA_APPKEY || null,
        username: contrato.SANKHYA_USERNAME || null,
        password: contrato.SANKHYA_PASSWORD || null, // Texto plano
        gemini: contrato.GEMINI_API_KEY || null,
        aiProvedor: contrato.AI_PROVEDOR || 'Gemini',
        aiModelo: contrato.AI_MODELO || 'gemini-2.0-flash',
        aiCredential: contrato.AI_CREDENTIAL || null,
        ativo: contrato.ATIVO ? 'S' : 'N',
        licencas: contrato.LICENCAS || null,
        id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      },
      { autoCommit: true }
    );

    const idEmpresa = (result.outBinds as any).id[0];
    console.log('✅ Contrato criado com ID:', idEmpresa);
    return idEmpresa;
  } catch (error: any) {
    console.error('❌ Erro ao criar contrato:', {
      message: error.message,
      code: error.errorNum,
      offset: error.offset
    });
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log('✅ Conexão Oracle fechada');
      } catch (err) {
        console.error('⚠️ Erro ao fechar conexão:', err);
      }
    }
  }
}

export async function atualizarContrato(id: number, contrato: Partial<Contrato>): Promise<void> {
  const connection = await getOracleConnection();

  try {
    await connection.execute(
      `UPDATE AD_CONTRATOS 
      SET 
        EMPRESA = :empresa,
        CNPJ = :cnpj,
        SANKHYA_TOKEN = :token,
        SANKHYA_APPKEY = :appkey,
        SANKHYA_USERNAME = :username,
        SANKHYA_PASSWORD = :password,
        GEMINI_API_KEY = :gemini,
        AI_PROVEDOR = :aiProvedor,
        AI_MODELO = :aiModelo,
        AI_CREDENTIAL = :aiCredential,
        ATIVO = :ativo,
        LICENCAS = :licencas
      WHERE ID_EMPRESA = :id`,
      {
        id,
        empresa: contrato.EMPRESA,
        cnpj: contrato.CNPJ,
        token: contrato.SANKHYA_TOKEN || null,
        appkey: contrato.SANKHYA_APPKEY || null,
        username: contrato.SANKHYA_USERNAME || null,
        password: contrato.SANKHYA_PASSWORD || null, // Texto plano
        gemini: contrato.GEMINI_API_KEY || null,
        aiProvedor: contrato.AI_PROVEDOR || 'Gemini',
        aiModelo: contrato.AI_MODELO || 'gemini-2.0-flash',
        aiCredential: contrato.AI_CREDENTIAL || null,
        ativo: contrato.ATIVO ? 'S' : 'N',
        licencas: contrato.LICENCAS || null
      },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function deletarContrato(id: number): Promise<void> {
  const connection = await getOracleConnection();

  try {
    await connection.execute(
      `DELETE FROM AD_CONTRATOS WHERE ID_EMPRESA = :id`,
      { id },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function atualizarAgendamentoSync(
  id: number,
  syncAtivo: boolean,
  intervaloMinutos: number
): Promise<void> {
  const connection = await getOracleConnection();

  try {
    const proximaSincronizacao = syncAtivo
      ? new Date(Date.now() + intervaloMinutos * 60000)
      : null;

    await connection.execute(
      `UPDATE AD_CONTRATOS 
      SET 
        SYNC_ATIVO = :syncAtivo,
        SYNC_INTERVALO_MINUTOS = :intervaloMinutos,
        PROXIMA_SINCRONIZACAO = :proximaSincronizacao
      WHERE ID_EMPRESA = :id`,
      {
        id,
        syncAtivo: syncAtivo ? 'S' : 'N',
        intervaloMinutos,
        proximaSincronizacao
      },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function atualizarUltimaSincronizacao(id: number): Promise<void> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `SELECT SYNC_INTERVALO_MINUTOS FROM AD_CONTRATOS WHERE ID_EMPRESA = :id`,
      { id },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (!result.rows || result.rows.length === 0) return;

    const intervalo = (result.rows[0] as any).SYNC_INTERVALO_MINUTOS || 120;
    const proximaSincronizacao = new Date(Date.now() + intervalo * 60000);

    await connection.execute(
      `UPDATE AD_CONTRATOS 
      SET 
        ULTIMA_SINCRONIZACAO = CURRENT_TIMESTAMP,
        PROXIMA_SINCRONIZACAO = :proximaSincronizacao
      WHERE ID_EMPRESA = :id`,
      { id, proximaSincronizacao },
      { autoCommit: true }
    );
  } finally {
    await connection.close();
  }
}

export async function buscarContratosParaSincronizar(): Promise<Contrato[]> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `SELECT 
        ID_EMPRESA, 
        EMPRESA, 
        CNPJ, 
        SANKHYA_TOKEN, 
        SANKHYA_APPKEY, 
        SANKHYA_USERNAME, 
        SANKHYA_PASSWORD, 
        GEMINI_API_KEY, 
        ATIVO,
        IS_SANDBOX,
        SYNC_ATIVO,
        SYNC_INTERVALO_MINUTOS,
        ULTIMA_SINCRONIZACAO,
        PROXIMA_SINCRONIZACAO
      FROM AD_CONTRATOS 
      WHERE SYNC_ATIVO = 'S' 
        AND ATIVO = 'S'
        AND (PROXIMA_SINCRONIZACAO IS NULL OR PROXIMA_SINCRONIZACAO <= CURRENT_TIMESTAMP)
      ORDER BY PROXIMA_SINCRONIZACAO ASC NULLS FIRST`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const contratos = (result.rows as any[]).map(row => ({
      ID_EMPRESA: row.ID_EMPRESA,
      EMPRESA: row.EMPRESA,
      CNPJ: row.CNPJ,
      SANKHYA_TOKEN: row.SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: row.SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: row.SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: row.SANKHYA_PASSWORD || '',
      GEMINI_API_KEY: row.GEMINI_API_KEY || '',
      ATIVO: row.ATIVO === 'S',
      IS_SANDBOX: row.IS_SANDBOX === 'S',
      SYNC_ATIVO: row.SYNC_ATIVO === 'S',
      SYNC_INTERVALO_MINUTOS: row.SYNC_INTERVALO_MINUTOS || 120,
      ULTIMA_SINCRONIZACAO: row.ULTIMA_SINCRONIZACAO,
      PROXIMA_SINCRONIZACAO: row.PROXIMA_SINCRONIZACAO
    }));

    return contratos;
  } finally {
    await connection.close();
  }
}

export async function buscarContratoPorId(id: number): Promise<Contrato | null> {
  const connection = await getOracleConnection();

  try {
    const result = await connection.execute(
      `SELECT 
        ID_EMPRESA, 
        EMPRESA, 
        CNPJ, 
        SANKHYA_TOKEN, 
        SANKHYA_APPKEY, 
        SANKHYA_USERNAME, 
        SANKHYA_PASSWORD,
        OAUTH_CLIENT_ID,
        OAUTH_CLIENT_SECRET,
        OAUTH_X_TOKEN,
        AUTH_TYPE,
        GEMINI_API_KEY, 
        AI_PROVEDOR,
        AI_MODELO,
        AI_CREDENTIAL,
        ATIVO,
        IS_SANDBOX,
        LICENCAS
      FROM AD_CONTRATOS 
      WHERE ID_EMPRESA = :id`,
      { id },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (!result.rows || result.rows.length === 0) {
      return null;
    }

    const row = result.rows[0] as any;
    const isSandbox = row.IS_SANDBOX === 'S';

    console.log(`🔍 [Oracle] buscarContratoPorId(${id}) - IS_SANDBOX no BD: '${row.IS_SANDBOX}', convertido para boolean: ${isSandbox}`);

    return {
      ID_EMPRESA: row.ID_EMPRESA,
      EMPRESA: row.EMPRESA,
      CNPJ: row.CNPJ,
      SANKHYA_TOKEN: row.SANKHYA_TOKEN || '',
      SANKHYA_APPKEY: row.SANKHYA_APPKEY || '',
      SANKHYA_USERNAME: row.SANKHYA_USERNAME || '',
      SANKHYA_PASSWORD: row.SANKHYA_PASSWORD || '',
      OAUTH_CLIENT_ID: row.OAUTH_CLIENT_ID || '',
      OAUTH_CLIENT_SECRET: row.OAUTH_CLIENT_SECRET || '',
      OAUTH_X_TOKEN: row.OAUTH_X_TOKEN || '',
      AUTH_TYPE: row.AUTH_TYPE || 'LEGACY',
      GEMINI_API_KEY: row.GEMINI_API_KEY || '',
      AI_PROVEDOR: row.AI_PROVEDOR || 'Gemini',
      AI_MODELO: row.AI_MODELO || 'gemini-2.0-flash',
      AI_CREDENTIAL: row.AI_CREDENTIAL || '',
      ATIVO: row.ATIVO === 'S',
      IS_SANDBOX: isSandbox,
      LICENCAS: row.LICENCAS
    };
  } finally {
    await connection.close();
  }
}