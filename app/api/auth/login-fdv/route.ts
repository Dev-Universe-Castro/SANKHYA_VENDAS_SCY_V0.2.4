
import { NextResponse } from 'next/server';
import { usuariosFDVService } from '@/lib/usuarios-fdv-service';
import { cryptoService } from '@/lib/crypto-service';

export async function POST(request: Request) {
  try {
    const { email, password } = await request.json();

    if (!email || !password) {
      return NextResponse.json(
        { error: 'Email e senha são obrigatórios' },
        { status: 400 }
      );
    }

    console.log("🔐 [login-fdv] Tentativa de login:", { email, timestamp: new Date().toISOString() });
    
    // Buscar todos os usuários com esse email (pode haver em várias empresas)
    const usuarios = await usuariosFDVService.getAll();
    const usuariosComEmail = usuarios.filter(u => u.EMAIL.toLowerCase() === email.toLowerCase());

    console.log("📋 [login-fdv] Resultado da busca:", { 
      encontrados: usuariosComEmail.length,
      usuarios: usuariosComEmail.map(u => ({ 
        id: u.CODUSUARIO, 
        nome: u.NOME, 
        funcao: u.FUNCAO,
        status: u.STATUS 
      }))
    });

    if (usuariosComEmail.length === 0) {
      console.log("❌ [login-fdv] Nenhum usuário encontrado com email:", email);
      return NextResponse.json(
        { error: 'Email ou senha inválidos' },
        { status: 401 }
      );
    }

    // Encontrar primeiro usuário ativo que seja Administrador
    const usuario = usuariosComEmail.find((u) => 
      u.STATUS === 'ativo' && 
      u.FUNCAO === 'Administrador'
    );

    if (!usuario) {
      console.log("❌ [login-fdv] Nenhum administrador ativo encontrado");
      return NextResponse.json(
        { error: 'Acesso restrito a administradores' },
        { status: 403 }
      );
    }

    // Log completo do usuário para debug
    console.log("👤 [login-fdv] Dados completos do usuário:", JSON.stringify(usuario, null, 2));

    // Validar senha (comparação direta sem criptografia)
    const senhaDigitada = password.trim();
    const senhaBanco = usuario.SENHA?.trim() || '';
    
    console.log("🔍 [login-fdv] Comparando senhas:", { 
      digitada: senhaDigitada,
      banco: senhaBanco,
      senhaType: typeof usuario.SENHA,
      senhaIsNull: usuario.SENHA === null,
      senhaIsUndefined: usuario.SENHA === undefined,
      match: senhaDigitada === senhaBanco
    });

    const isPasswordValid = senhaDigitada === senhaBanco;

    if (!isPasswordValid) {
      console.log("❌ [login-fdv] Senha inválida - digitada !== banco");
      return NextResponse.json(
        { error: 'Email ou senha inválidos' },
        { status: 401 }
      );
    }

    console.log("✅ [login-fdv] Login bem-sucedido:", usuario.NOME);

    // Converter formato de usuário FDV para formato esperado pelo sistema
    const userFormatted = {
      id: usuario.CODUSUARIO,
      name: usuario.NOME,
      email: usuario.EMAIL,
      role: 'Administrador',
      status: usuario.STATUS,
      avatar: usuario.AVATAR,
      idEmpresa: usuario.ID_EMPRESA,
      empresa: usuario.EMPRESA,
      cnpj: usuario.CNPJ,
      codVend: usuario.CODVEND
    };

    // Criar resposta com cookie de sessão
    const response = NextResponse.json({ user: userFormatted });
    response.cookies.set('user', JSON.stringify(userFormatted), {
      httpOnly: false,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24 * 7,
      path: '/'
    });

    return response;

  } catch (error: any) {
    console.error('❌ Erro no login FDV:', error);
    return NextResponse.json(
      { error: 'Erro ao fazer login. Tente novamente.' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
