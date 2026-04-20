
import { NextResponse } from 'next/server';
import { getCacheService } from '@/lib/redis-cache-wrapper';

const API_LOGS_KEY = 'global:server:api_logs:sankhya';
const MAX_LOGS = 500; // Histórico de 500 logs globais (persistidos por 7 dias)

export async function addApiLog(log: {
  method: string;
  url: string;
  status: number;
  duration: number;
  tokenUsed: boolean;
  userId?: string;
  userName?: string;
  error?: string;
}) {
  const newLog = {
    id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    timestamp: new Date().toISOString(),
    ...log,
    error: log.error || null
  };

  try {
    const cacheService = await getCacheService();
    const existingLogs = await cacheService.get<any[]>(API_LOGS_KEY) || [];
    existingLogs.unshift(newLog);
    const updatedLogs = existingLogs.slice(0, MAX_LOGS);
    await cacheService.set(API_LOGS_KEY, updatedLogs, 7 * 24 * 60 * 60 * 1000);

    const statusEmoji = log.status >= 400 ? '❌' : '✅';
    const errorInfo = log.error ? ` | Erro: ${log.error}` : '';
    const userInfo = log.userName ? ` | Usuário: ${log.userName}` : '';
    console.log(`${statusEmoji} [GLOBAL SERVER LOG] ${log.method} ${log.url} - ${log.status}${userInfo}${errorInfo}`);
  } catch (error) {
    console.error('❌ Erro ao adicionar log global do servidor:', error);
  }
}

export async function GET() {
  try {
    console.log('📋 [GLOBAL] Buscando logs globais do servidor...');
    const cacheService = await getCacheService();
    const apiLogs = await cacheService.get<any[]>(API_LOGS_KEY) || [];

    console.log(`✅ [API /admin/api-logs] ${apiLogs.length} logs globais encontrados`);

    return NextResponse.json({
      logs: apiLogs,
      total: apiLogs.length,
      maxLogs: MAX_LOGS,
      isGlobal: true,
      persistenceDays: 7
    }, {
      headers: {
        'Cache-Control': 'no-store, no-cache, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      }
    });
  } catch (error) {
    console.error('❌ [API /admin/api-logs] Erro ao obter logs globais:', error);
    return NextResponse.json({
      error: 'Erro interno do servidor',
      logs: [],
      total: 0
    }, { status: 500 });
  }
}

export const dynamic = 'force-dynamic';
