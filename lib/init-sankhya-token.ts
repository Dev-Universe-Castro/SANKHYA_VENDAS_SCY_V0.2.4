
import { obterToken } from './sankhya-api';
import { adicionarLog } from './api-logger';

/**
 * Inicializa o token do Sankhya automaticamente ao iniciar o servidor
 * Gera tokens para todos os contratos ativos
 */
export async function initSankhyaToken() {
  try {
    console.log('🔐 [INIT-TOKEN] Iniciando autenticação automática com Sankhya...');
    adicionarLog('INFO', 'Iniciando autenticação automática com Sankhya');
    
    // Importar dinamicamente para evitar dependência circular
    const { listarContratos } = await import('./oracle-service');
    const contratos = await listarContratos();
    const contratosAtivos = contratos.filter((c: any) => c.ATIVO === true);
    
    console.log(`🔐 [INIT-TOKEN] ${contratosAtivos.length} contratos ativos encontrados`);
    
    // Gerar token para cada contrato ativo
    for (const contrato of contratosAtivos) {
      try {
        const authType = contrato.AUTH_TYPE || 'LEGACY';
        console.log(`🔐 [INIT-TOKEN] Gerando token ${authType} para contrato ${contrato.ID_EMPRESA} - ${contrato.EMPRESA}`);
        await obterToken(contrato.ID_EMPRESA, false);
        console.log(`✅ [INIT-TOKEN] Token gerado para ${contrato.EMPRESA}`);
      } catch (erro: any) {
        console.error(`❌ [INIT-TOKEN] Erro ao gerar token para ${contrato.EMPRESA}:`, erro.message);
      }
    }
    
    console.log('✅ [INIT-TOKEN] Inicialização de tokens concluída');
    console.log('📅 [INIT-TOKEN] Concluído em:', new Date().toISOString());
    
    adicionarLog('SUCCESS', 'Tokens Sankhya inicializados com sucesso', {
      geradoEm: new Date().toISOString(),
      totalContratos: contratosAtivos.length
    });
    
    return true;
  } catch (erro: any) {
    console.error('❌ [INIT-TOKEN] Erro ao inicializar tokens do Sankhya:', erro.message);
    console.log('⚠️ [INIT-TOKEN] O sistema continuará, mas os tokens serão obtidos na primeira requisição');
    
    adicionarLog('ERROR', 'Erro ao inicializar tokens do Sankhya', {
      erro: erro.message
    });
    
    return false;
  }
}
