// Wrapper para garantir que Redis só é importado no servidor
import type { RedisCacheService } from './redis-cache-service';

let redisCacheServiceInstance: any = null;
let initPromise: Promise<any> | null = null;

// Cache L1 em memória com TTL de 25 minutos (maior que o token de 20min)
interface MemoryCacheEntry<T> {
  data: T;
  expiresAt: number;
}

const memoryCache = new Map<string, MemoryCacheEntry<any>>();
const MEMORY_TTL = 25 * 60 * 1000; // 25 minutos

// Cleanup de memória a cada 5 minutos
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [key, entry] of memoryCache.entries()) {
    if (entry.expiresAt < now) {
      memoryCache.delete(key);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    console.log(`🧹 [Cache L1] Limpou ${cleaned} entradas expiradas`);
  }
}, 5 * 60 * 1000);

// Mock para o cliente com cache L1 persistente
const mockCache = {
  get: async <T>(key: string): Promise<T | null> => {
    const entry = memoryCache.get(key);
    if (entry && entry.expiresAt > Date.now()) {
      return entry.data as T;
    }
    if (entry) {
      memoryCache.delete(key);
    }
    return null;
  },
  set: async <T>(key: string, data: T, ttlSeconds?: number): Promise<void> => {
    const ttl = ttlSeconds ? ttlSeconds * 1000 : MEMORY_TTL;
    memoryCache.set(key, {
      data,
      expiresAt: Date.now() + ttl
    });
  },
  delete: async (key: string): Promise<void> => {
    memoryCache.delete(key);
  },
  has: async (key: string): Promise<boolean> => {
    const entry = memoryCache.get(key);
    if (entry && entry.expiresAt > Date.now()) {
      return true;
    }
    if (entry) {
      memoryCache.delete(key);
    }
    return false;
  },
  clear: async (): Promise<void> => {
    memoryCache.clear();
  },
  cleanup: async (): Promise<void> => {
    const now = Date.now();
    for (const [key, entry] of memoryCache.entries()) {
      if (entry.expiresAt < now) {
        memoryCache.delete(key);
      }
    }
  },
  invalidatePattern: async (pattern: string): Promise<number> => {
    let count = 0;
    for (const key of memoryCache.keys()) {
      if (key.includes(pattern)) {
        memoryCache.delete(key);
        count++;
      }
    }
    return count;
  },
  invalidateParceiros: async () => mockCache.invalidatePattern('parceiros'),
  invalidateProdutos: async () => mockCache.invalidatePattern('produtos'),
  invalidateEstoque: async () => mockCache.invalidatePattern('estoque'),
  invalidatePrecos: async () => mockCache.invalidatePattern('preco'),
  invalidatePedidos: async () => mockCache.invalidatePattern('pedidos'),
  getStats: async () => ({
    memorySize: memoryCache.size,
    redisSize: 0,
    totalSize: memoryCache.size,
    usingRedis: false,
    memoryKeys: Array.from(memoryCache.keys()),
    redisKeys: [],
    ttlConfig: { MEMORY_TTL }
  }),
  mget: async <T>(keys: string[]): Promise<Map<string, T>> => {
    const results = new Map<string, T>();
    const now = Date.now();
    for (const key of keys) {
      const entry = memoryCache.get(key);
      if (entry && entry.expiresAt > now) {
        results.set(key, entry.data as T);
      }
    }
    return results;
  }
};

// Função para obter o serviço de cache
export async function getCacheService() {
  // Se estiver no cliente, retorna o mock
  if (typeof window !== 'undefined') {
    return mockCache;
  }

  // Se já temos a instância, retorna
  if (redisCacheServiceInstance) {
    return redisCacheServiceInstance;
  }

  // Se está inicializando, aguarda
  if (initPromise) {
    return initPromise;
  }

  // Inicializa o serviço
  initPromise = (async () => {
    try {
      const module = await import('./redis-cache-service');
      redisCacheServiceInstance = module.redisCacheService;
      return redisCacheServiceInstance;
    } catch (error) {
      console.warn('⚠️ Redis não disponível, usando cache em memória');
      return mockCache;
    }
  })();

  return initPromise;
}