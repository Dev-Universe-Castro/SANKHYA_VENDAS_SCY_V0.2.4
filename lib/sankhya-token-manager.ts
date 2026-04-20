
import { obterToken } from './sankhya-api';

/**
 * Gerenciador centralizado de token Sankhya
 * Todos os serviços devem usar esta função para obter o token
 * O token é gerenciado globalmente via Redis
 * @param contratoId - ID do contrato (opcional, usa contrato ativo se não especificado)
 * @param forceRefresh - Força renovação do token
 */
export async function getSankhyaToken(contratoId?: number, forceRefresh = false): Promise<string> {
  return obterToken(contratoId, forceRefresh);
}

/**
 * Headers padrão para autenticação nas requisições Sankhya
 * @param contratoId - ID do contrato (opcional, usa contrato ativo se não especificado)
 */
export async function getSankhyaAuthHeaders(contratoId?: number): Promise<{ Authorization: string; 'Content-Type': string }> {
  const token = await getSankhyaToken(contratoId);
  return {
    'Authorization': `Bearer ${token}`,
    'Content-Type': 'application/json'
  };
}
