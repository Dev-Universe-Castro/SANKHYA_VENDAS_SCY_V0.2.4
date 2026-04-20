
import { NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { consultarFunis } from '@/lib/funis-service';

export async function GET(request: Request) {
  try {
    console.log('📡 API - Iniciando consulta de funis...');
    console.log('📡 API - URL:', request.url);
    console.log('📡 API - Headers:', Object.fromEntries(request.headers.entries()));
    
    // Obter usuário autenticado do cookie
    const cookieStore = await cookies();
    const allCookies = cookieStore.getAll();
    console.log('🍪 Todos os cookies disponíveis:', allCookies.map(c => c.name));
    
    const userCookie = cookieStore.get('user');
    
    if (!userCookie) {
      console.error('❌ Cookie de usuário não encontrado');
      console.error('❌ Cookies disponíveis:', allCookies);
      return NextResponse.json({ 
        error: 'Não autenticado',
        details: 'Cookie de sessão não encontrado'
      }, { status: 401 });
    }

    console.log('🍪 Cookie encontrado:', userCookie.name);

    let currentUser;
    try {
      currentUser = JSON.parse(userCookie.value);
      console.log('🍪 Cookie do usuário parseado:', JSON.stringify(currentUser, null, 2));
    } catch (e) {
      console.error('❌ Erro ao parsear cookie de usuário:', e);
      console.error('❌ Valor do cookie:', userCookie.value);
      return NextResponse.json({ 
        error: 'Sessão inválida',
        details: 'Não foi possível processar os dados da sessão'
      }, { status: 401 });
    }

    // Verificar múltiplas variações de admin
    const isAdmin = currentUser.role === 'Administrador' || 
                    currentUser.role === 'Admin' || 
                    currentUser.role === 'admin' ||
                    currentUser.role === 'ADMINISTRADOR';
    
    const codUsuario = parseInt(currentUser.id) || currentUser.id;

    console.log(`👤 Usuário autenticado: ${currentUser.name || 'Sem nome'} (ID: ${codUsuario}, Role: ${currentUser.role}, Admin: ${isAdmin})`);

    // Se é admin, passar undefined para codUsuario para buscar todos os funis
    const funis = await consultarFunis(isAdmin ? undefined : codUsuario, isAdmin);
    console.log(`✅ API - ${funis.length} funis retornados`);
    
    // Debug: verificar o retorno completo
    if (funis.length === 0) {
      console.log('⚠️ Nenhum funil encontrado. Verificando permissões...');
      console.log('🔍 Parâmetros usados na busca:', { codUsuario, isAdmin });
    }
    
    return NextResponse.json(funis);
  } catch (error: any) {
    console.error('❌ API - Erro ao consultar funis:', error.message);
    console.error('Stack trace:', error.stack);
    return NextResponse.json(
      { 
        error: error.message || 'Erro ao consultar funis',
        details: 'Verifique a conexão com a API Sankhya'
      },
      { status: 500 }
    );
  }
}

// Desabilitar cache para esta rota
export const dynamic = 'force-dynamic';
export const revalidate = 0;
