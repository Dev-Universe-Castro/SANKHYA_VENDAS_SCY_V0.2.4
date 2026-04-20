import { config } from 'dotenv';
import { resolve } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Carregar variáveis de ambiente do config.env.local
const result = config({ path: resolve(__dirname, 'config.env.local') });

if (result.error) {
  console.error('❌ Erro ao carregar config.env.local:', result.error);
} else {
  console.log('✅ Variáveis carregadas do config.env.local:', Object.keys(result.parsed || {}).length);
}

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Não usar basePath para evitar duplicação de rotas
  output: 'standalone',
  // Excluir módulos do servidor do bundle do cliente
  webpack: (config, { isServer }) => {
    if (!isServer) {
      // Excluir redis e outros módulos do servidor do bundle do cliente
      config.resolve.fallback = {
        ...config.resolve.fallback,
        net: false,
        tls: false,
        dns: false,
        fs: false,
        redis: false,
        'node:assert': false,
        'node:buffer': false,
        'node:crypto': false,
        'node:events': false,
        'node:stream': false,
        'node:util': false,
      };
      
      // Ignorar módulos do Redis completamente no cliente
      config.externals = config.externals || [];
      config.externals.push({
        redis: 'commonjs redis',
        '@redis/client': 'commonjs @redis/client',
        '@redis/bloom': 'commonjs @redis/bloom',
        '@redis/json': 'commonjs @redis/json',
        '@redis/search': 'commonjs @redis/search',
        '@redis/time-series': 'commonjs @redis/time-series',
      });
    }
    return config;
  },
  env: {
    NEXT_PUBLIC_APP_URL: process.env.NEXT_PUBLIC_SITE_URL || 'http://crescimentoerp.nuvemdatacom.com.br:4000',
    SANKHYA_TOKEN: process.env.SANKHYA_TOKEN,
    SANKHYA_APPKEY: process.env.SANKHYA_APPKEY,
    SANKHYA_USERNAME: process.env.SANKHYA_USERNAME,
    SANKHYA_PASSWORD: process.env.SANKHYA_PASSWORD,
    GEMINI_API_KEY: process.env.GEMINI_API_KEY,
    NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL || 'http://crescimentoerp.nuvemdatacom.com.br:4000'
  },
  // Garantir que as URLs sejam geradas corretamente
  trailingSlash: false,
  eslint: {
    ignoreDuringBuilds: true,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // Otimizações de performance
  swcMinify: true,
  reactStrictMode: true,
  poweredByHeader: false,
  compress: true,
  experimental: {
    optimizePackageImports: ['lucide-react', '@radix-ui/react-icons'],
    optimizeCss: true,
    // Otimizações para alta concorrência
    cpus: 2,
    workerThreads: true,
    instrumentationHook: true,
    // Otimizações adicionais
    serverMinification: true,
  },
  // Cache agressivo
  cacheMaxMemorySize: 100 * 1024 * 1024, // 100MB
  // Compressão agressiva
  compress: true,
  generateEtags: true,
  // Otimizações de compilação
  compiler: {
    removeConsole: process.env.NODE_ENV === 'production',
  },
  // Configurações de cache agressivo para múltiplos usuários
  onDemandEntries: {
    maxInactiveAge: 120 * 1000, // 2 minutos
    pagesBufferLength: 10, // Mais páginas em cache
  },
  // Headers de cache otimizados
  async headers() {
    return [
      {
        source: '/:path*',
        headers: [
          {
            key: 'Cache-Control',
            value: 'no-cache, no-store, must-revalidate',
          },
        ],
      },
      {
        source: '/api/:path*',
        headers: [
          { key: 'Access-Control-Allow-Credentials', value: 'true' },
          { key: 'Access-Control-Allow-Origin', value: process.env.NEXT_PUBLIC_SITE_URL || '*' },
          { key: 'Access-Control-Allow-Methods', value: 'GET,DELETE,PATCH,POST,PUT,OPTIONS' },
          { key: 'Access-Control-Allow-Headers', value: 'X-CSRF-Token, X-Requested-With, Accept, Accept-Version, Content-Length, Content-MD5, Content-Type, Date, X-Api-Version, Cookie, Set-Cookie' },
        ],
      },
    ];
  },
}

export default nextConfig;
