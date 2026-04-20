import { NextResponse } from "next/server"
import { getOracleConnection } from "@/lib/oracle-service"
import { cryptoService } from "@/lib/crypto-service"
import oracledb from "oracledb"

export async function POST(request: Request) {
  let connection;

  try {
    const {
      empresa,
      cnpj,
      authType,
      sankhyaToken,
      sankhyaAppkey,
      sankhyaUsername,
      sankhyaPassword,
      oauthClientId,
      oauthClientSecret,
      oauthXToken,
      geminiApiKey,
      ativo,
      isSandbox,
      licencas,
      aiProvedor,
      aiModelo,
      aiCredential
    } = await request.json()

    console.log('📝 Criando contrato:', { empresa, cnpj, ativo, isSandbox, licencas });

    if (!empresa || !cnpj) {
      return NextResponse.json({ error: "Campos obrigatórios não preenchidos" }, { status: 400 })
    }

    connection = await getOracleConnection();

    // IMPORTANTE: SANKHYA_PASSWORD é armazenado em texto plano
    // A API Sankhya precisa do password em texto plano para autenticação
    const plainSankhyaPassword = sankhyaPassword || null;

    // Gemini API Key pode continuar criptografado (não é usado em autenticação externa)
    let hashedGeminiApiKey = null;
    if (geminiApiKey) {
      hashedGeminiApiKey = await cryptoService.hashPassword(geminiApiKey);
    }

    const result = await connection.execute(
      `INSERT INTO AD_CONTRATOS 
        (EMPRESA, CNPJ, AUTH_TYPE, SANKHYA_TOKEN, SANKHYA_APPKEY, SANKHYA_USERNAME, SANKHYA_PASSWORD, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_X_TOKEN, GEMINI_API_KEY, AI_PROVEDOR, AI_MODELO, AI_CREDENTIAL, ATIVO, IS_SANDBOX, LICENCAS)
      VALUES 
        (:empresa, :cnpj, :authType, :token, :appkey, :username, :password, :clientId, :clientSecret, :xToken, :gemini, :aiProvedor, :aiModelo, :aiCredential, :ativo, :isSandbox, :licencas)
      RETURNING ID_EMPRESA INTO :id`,
      {
        empresa,
        cnpj,
        authType: authType || 'LEGACY',
        token: sankhyaToken || null,
        appkey: sankhyaAppkey || null,
        username: sankhyaUsername || null,
        password: plainSankhyaPassword,
        clientId: oauthClientId || null,
        clientSecret: oauthClientSecret || null,
        xToken: oauthXToken || null,
        gemini: hashedGeminiApiKey,
        aiProvedor: aiProvedor || 'Gemini',
        aiModelo: aiModelo || 'gemini-2.0-flash',
        aiCredential: aiCredential || null,
        ativo: (ativo !== false) ? 'S' : 'N',
        isSandbox: (isSandbox !== false) ? 'S' : 'N',
        licencas: licencas || 0,
        id: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      },
      { autoCommit: true }
    );

    const idEmpresa = (result.outBinds as any).id[0];
    console.log('✅ Contrato criado com ID:', idEmpresa);

    return NextResponse.json({ success: true, id: idEmpresa })
  } catch (error: any) {
    console.error("❌ Erro ao criar contrato:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}