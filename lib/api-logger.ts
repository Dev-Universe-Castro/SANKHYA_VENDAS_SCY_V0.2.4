/**
 * Sistema de logs de requisições API
 * Armazena os últimos 50 logs em memória
 */

interface ApiLog {
  timestamp: string;
  tipo: 'INFO' | 'ERROR' | 'SUCCESS' | 'WARNING';
  mensagem: string;
  detalhes?: any;
}

// Armazenamento em memória dos logs
const logs: ApiLog[] = [];
const MAX_LOGS = 50;

/**
 * Adiciona um log ao sistema
 */
export function adicionarLog(tipo: ApiLog['tipo'], mensagem: string, detalhes?: any) {
  const log: ApiLog = {
    timestamp: new Date().toISOString(),
    tipo,
    mensagem,
    detalhes
  };

  logs.unshift(log); // Adiciona no início

  // Limita o tamanho do array
  if (logs.length > MAX_LOGS) {
    logs.pop();
  }

  // Também loga no console do servidor
  const emoji = {
    INFO: 'ℹ️',
    ERROR: '❌',
    SUCCESS: '✅',
    WARNING: '⚠️'
  }[tipo];

  console.log(`${emoji} [API-LOG] ${mensagem}`, detalhes ? detalhes : '');
}

/**
 * Retorna todos os logs
 */
export function obterLogs(): ApiLog[] {
  return [...logs];
}

/**
 * Limpa todos os logs
 */
export function limparLogs() {
  logs.length = 0;
}

// Função para registrar requisições (pode ser expandida)
export async function logApiRequest(data: any) {
  // Por enquanto, apenas loga no console
  console.log('📝 [API-LOG]', data);
}