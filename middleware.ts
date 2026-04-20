import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(request: NextRequest) {
  const path = request.nextUrl.pathname
  const origin = request.headers.get('origin')

  console.log('🔒 Middleware - Path:', path)
  console.log('🌐 Middleware - Origin:', origin)

  // Permitir acesso a arquivos estáticos (imagens, fonts, etc.)
  if (
    path.startsWith('/_next/') ||
    path.startsWith('/static/') ||
    path.match(/\.(png|jpg|jpeg|gif|svg|ico|webp|woff|woff2|ttf|eot)$/)
  ) {
    console.log('📁 Middleware - Arquivo estático, permitindo acesso')
    return NextResponse.next()
  }

  // Rotas públicas que não precisam de autenticação
  const publicPaths = ['/', '/register', '/api/usuarios/register', '/api/auth/login', '/admin-panel']
  const isPublicPath = publicPaths.includes(path)

  // Pega o cookie de usuário
  const userCookie = request.cookies.get('user')?.value

  console.log('🍪 Middleware - Cookie presente:', !!userCookie)
  console.log('🍪 Middleware - Todos os cookies:', request.cookies.getAll().map(c => c.name))

  // Se não está autenticado e tenta acessar rota protegida
  if (!isPublicPath && !userCookie) {
    console.log('❌ Middleware - Não autenticado, redirecionando para /')
    return NextResponse.redirect(new URL('/', request.url))
  }

  // Se está autenticado e tenta acessar login/register
  if (isPublicPath && userCookie && path !== '/register') {
    console.log('✅ Middleware - Autenticado, redirecionando para /dashboard')
    const dashboardUrl = new URL('/dashboard', request.url)
    return NextResponse.redirect(dashboardUrl)
  }

  console.log('✅ Middleware - Permitindo acesso')
  const response = NextResponse.next()

  // Garantir que os cookies sejam enviados corretamente
  response.headers.set('Access-Control-Allow-Credentials', 'true')

  return response
}

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - api (API routes)
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     * - admin-panel (admin panel routes)
     */
    '/((?!api|_next/static|_next/image|favicon.ico|admin-panel).*)',
  ],
}