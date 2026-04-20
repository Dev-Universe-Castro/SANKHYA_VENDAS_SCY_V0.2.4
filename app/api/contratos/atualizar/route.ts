import { NextResponse } from "next/server"
import { getOracleConnection } from "@/lib/oracle-service"
import { cryptoService } from "@/lib/crypto-service"

export async function POST(request: Request) {
  let connection;

  try {
    const {
      id,
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

    console.log('📝 Dados recebidos:', { id, empresa, cnpj, ativo, isSandbox, licencas });

    if (!id || !empresa || !cnpj) {
      return NextResponse.json({ error: "Campos obrigatórios não preenchidos" }, { status: 400 })
    }

    connection = await getOracleConnection();

    // Montar SQL dinamicamente
    const updates = [];
    const binds: any = { id };

    updates.push('EMPRESA = :empresa');
    binds.empresa = empresa;

    updates.push('CNPJ = :cnpj');
    binds.cnpj = cnpj;

    if (authType !== undefined) {
      updates.push('AUTH_TYPE = :authType');
      binds.authType = authType;
    }

    if (sankhyaToken !== undefined) {
      updates.push('SANKHYA_TOKEN = :sankhyaToken');
      binds.sankhyaToken = sankhyaToken || null;
    }

    if (sankhyaAppkey !== undefined) {
      updates.push('SANKHYA_APPKEY = :sankhyaAppkey');
      binds.sankhyaAppkey = sankhyaAppkey || null;
    }

    if (sankhyaUsername !== undefined) {
      updates.push('SANKHYA_USERNAME = :sankhyaUsername');
      binds.sankhyaUsername = sankhyaUsername || null;
    }

    if (sankhyaPassword !== undefined) {
      updates.push('SANKHYA_PASSWORD = :sankhyaPassword');
      // IMPORTANTE: SANKHYA_PASSWORD é armazenado em texto plano
      binds.sankhyaPassword = sankhyaPassword || null;
    }

    if (oauthClientId !== undefined) {
      updates.push('OAUTH_CLIENT_ID = :oauthClientId');
      binds.oauthClientId = oauthClientId || null;
    }

    if (oauthClientSecret !== undefined) {
      updates.push('OAUTH_CLIENT_SECRET = :oauthClientSecret');
      binds.oauthClientSecret = oauthClientSecret || null;
    }

    if (oauthXToken !== undefined) {
      updates.push('OAUTH_X_TOKEN = :oauthXToken');
      binds.oauthXToken = oauthXToken || null;
    }

    if (geminiApiKey !== undefined) {
      updates.push('GEMINI_API_KEY = :geminiApiKey');
      binds.geminiApiKey = geminiApiKey || null;
    }

    if (ativo !== undefined) {
      updates.push('ATIVO = :ativo');
      binds.ativo = ativo ? 'S' : 'N';
    }

    if (isSandbox !== undefined) {
      updates.push('IS_SANDBOX = :isSandbox');
      binds.isSandbox = isSandbox ? 'S' : 'N';
    }

    if (licencas !== undefined) {
      updates.push('LICENCAS = :licencas');
      binds.licencas = licencas || 0;
    }

    if (aiProvedor !== undefined) {
      updates.push('AI_PROVEDOR = :aiProvedor');
      binds.aiProvedor = aiProvedor || 'Gemini';
    }

    if (aiModelo !== undefined) {
      updates.push('AI_MODELO = :aiModelo');
      binds.aiModelo = aiModelo || 'gemini-2.0-flash';
    }

    if (aiCredential !== undefined) {
      updates.push('AI_CREDENTIAL = :aiCredential');
      binds.aiCredential = aiCredential || null;
    }

    updates.push('DATA_ATUALIZACAO = CURRENT_TIMESTAMP');

    const sql = `UPDATE AD_CONTRATOS SET ${updates.join(', ')} WHERE ID_EMPRESA = :id`;

    console.log('🔧 SQL:', sql);
    console.log('🔧 Binds:', binds);

    await connection.execute(sql, binds, { autoCommit: true });

    console.log('✅ Contrato atualizado com sucesso');
    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error("❌ Erro ao atualizar contrato:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}