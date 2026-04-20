import { buscarContratosParaSincronizar, atualizarUltimaSincronizacao } from './oracle-service';
import { salvarLogSincronizacao } from './sync-logs-service';

// Imports estáticos de todos os serviços de sincronização
import { sincronizarParceirosTotal } from './sync-parceiros-service';
import { sincronizarProdutosTotal } from './sync-produtos-service';
import { sincronizarTiposNegociacaoTotal } from './sync-tipos-negociacao-service';
import { sincronizarTiposOperacaoTotal } from './sync-tipos-operacao-service';
import { sincronizarEstoquesPorEmpresa } from './sync-estoques-service';

import { sincronizarExcecaoPrecoTotal } from './sync-excecao-preco-service';
import { sincronizarTabelaPrecosTotal } from './sync-tabelas-precos-service';
import { sincronizarVendedoresTotal } from './sync-vendedores-service';
import { sincronizarMarcasTotal } from './sync-marcas-service';
import { sincronizarGruposProdutosTotal } from './sync-grupos-produtos-service';
import { sincronizarBairrosTotal } from './sync-bairros-service';
import { sincronizarCidadesTotal } from './sync-cidades-service';
import { sincronizarEmpresasPorEmpresa } from './sync-empresas-service';
import { sincronizarRegioesPorEmpresa } from './sync-regioes-service';
import { sincronizarEstadosPorEmpresa } from './sync-estados-service';
import { sincronizarComplementoParcTotal } from './sync-complemento-parc-service';

interface SyncQueueItem {
  idEmpresa: number;
  empresa: string;
  timestamp: Date;
}

interface TabelaSincronizacao {
  nome: string;
  rota: string;
  maxTentativas: number;
  funcaoSync: (idEmpresa: number, empresa: string) => Promise<any>;
}

class SyncQueueService {
  private queue: SyncQueueItem[] = [];
  private isProcessing: boolean = false;
  private intervalId: NodeJS.Timeout | null = null;
  private isSyncRunning: boolean = false;
  private contractsInProcessing: Set<number> = new Set();

  private tabelas: TabelaSincronizacao[] = [
    {
      nome: 'Parceiros',
      rota: '/api/sync/parceiros',
      maxTentativas: 3,
      funcaoSync: sincronizarParceirosTotal
    },
    {
      nome: 'Produtos',
      rota: '/api/sync/produtos',
      maxTentativas: 3,
      funcaoSync: sincronizarProdutosTotal
    },
    {
      nome: 'Tipos de Negociação',
      rota: '/api/sync/tipos-negociacao',
      maxTentativas: 3,
      funcaoSync: sincronizarTiposNegociacaoTotal
    },
    {
      nome: 'Tipos de Operação',
      rota: '/api/sync/tipos-operacao',
      maxTentativas: 3,
      funcaoSync: sincronizarTiposOperacaoTotal
    },
    {
      nome: 'Estoques',
      rota: '/api/sync/estoques',
      maxTentativas: 3,
      funcaoSync: sincronizarEstoquesPorEmpresa
    },
    {
      nome: 'Tabela de Preços',
      rota: '/api/sync/tabela-precos',
      maxTentativas: 3,
      funcaoSync: sincronizarTabelaPrecosTotal
    },
    {
      nome: 'Exceção de Preços',
      rota: '/api/sync/excecao-preco',
      maxTentativas: 3,
      funcaoSync: sincronizarExcecaoPrecoTotal
    },
    {
      nome: 'Vendedores',
      rota: '/api/sync/vendedores',
      maxTentativas: 3,
      funcaoSync: sincronizarVendedoresTotal
    },
    {
      nome: 'Marcas',
      rota: '/api/sync/marcas',
      maxTentativas: 3,
      funcaoSync: sincronizarMarcasTotal
    },
    {
      nome: 'Grupos de Produtos',
      rota: '/api/sync/grupos-produtos',
      maxTentativas: 3,
      funcaoSync: sincronizarGruposProdutosTotal
    },
    {
      nome: 'Bairros',
      rota: '/api/sync/bairros',
      maxTentativas: 3,
      funcaoSync: sincronizarBairrosTotal
    },
    {
      nome: 'Cidades',
      rota: '/api/sync/cidades',
      maxTentativas: 3,
      funcaoSync: sincronizarCidadesTotal
    },
    {
      nome: 'Empresas',
      rota: '/api/sync/empresas',
      maxTentativas: 3,
      funcaoSync: sincronizarEmpresasPorEmpresa
    },
    {
      nome: 'Regiões',
      rota: '/api/sync/regioes',
      maxTentativas: 3,
      funcaoSync: sincronizarRegioesPorEmpresa
    },
    {
      nome: 'Estados',
      rota: '/api/sync/estados',
      maxTentativas: 3,
      funcaoSync: sincronizarEstadosPorEmpresa
    },
    {
      nome: 'Complemento Parceiro',
      rota: '/api/sync/complemento-parceiro',
      maxTentativas: 3,
      funcaoSync: sincronizarComplementoParcTotal
    }
  ];

  start() {
    if (this.intervalId) {
      console.log('⚠️ Fila de sincronização já está rodando');
      return;
    }

    console.log('🚀 Iniciando serviço de fila de sincronização');

    // Verificar a cada minuto se há sincronizações pendentes
    this.intervalId = setInterval(async () => {
      await this.checkAndQueueSyncs();
    }, 60000); // 1 minuto

    // Executar primeira verificação imediatamente
    this.checkAndQueueSyncs();
  }

  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log('🛑 Serviço de fila de sincronização parado');
    }
  }

  private async checkAndQueueSyncs() {
    try {
      const contratos = await buscarContratosParaSincronizar();

      if (contratos.length === 0) {
        return;
      }

      console.log(`📋 ${contratos.length} contrato(s) encontrado(s) para sincronização`);

      for (const contrato of contratos) {
        // Verificar se já não está na fila OU em processamento
        const jaExisteNaFila = this.queue.some(item => item.idEmpresa === contrato.ID_EMPRESA);
        const estaEmProcessamento = this.contractsInProcessing.has(contrato.ID_EMPRESA);

        if (!jaExisteNaFila && !estaEmProcessamento) {
          this.queue.push({
            idEmpresa: contrato.ID_EMPRESA,
            empresa: contrato.EMPRESA,
            timestamp: new Date()
          });

          console.log(`➕ Adicionado à fila: ${contrato.EMPRESA} (ID: ${contrato.ID_EMPRESA})`);
        } else if (estaEmProcessamento) {
          console.log(`⚠️ Contrato ${contrato.EMPRESA} já está sendo sincronizado - ignorando`);
        }
      }

      // Processar fila se não estiver processando
      if (!this.isProcessing && !this.isSyncRunning && this.queue.length > 0) {
        this.processQueue();
      }
    } catch (error) {
      console.error('❌ Erro ao verificar sincronizações pendentes:', error);
    }
  }

  private async processQueue() {
    if (this.isProcessing || this.isSyncRunning) {
      console.log('⚠️ Já existe uma sincronização em andamento');
      return;
    }

    this.isProcessing = true;
    this.isSyncRunning = true;

    while (this.queue.length > 0) {
      const item = this.queue.shift();

      if (!item) break;

      // Marcar como em processamento
      this.contractsInProcessing.add(item.idEmpresa);

      console.log(`\n${'='.repeat(60)}`);
      console.log(`🔄 Processando sincronização: ${item.empresa}`);
      console.log(`📊 Contratos restantes na fila: ${this.queue.length}`);
      console.log(`📋 Contratos em processamento: ${this.contractsInProcessing.size}`);
      console.log(`${'='.repeat(60)}\n`);

      try {
        await this.syncAllTables(item.idEmpresa, item.empresa);
        await atualizarUltimaSincronizacao(item.idEmpresa);
        console.log(`✅ Sincronização concluída: ${item.empresa}\n`);
      } catch (error) {
        console.error(`❌ Erro na sincronização de ${item.empresa}:`, error);
      } finally {
        // Remover do conjunto de processamento
        this.contractsInProcessing.delete(item.idEmpresa);
      }
    }

    this.isProcessing = false;
    this.isSyncRunning = false;
    console.log('✨ Fila de sincronização processada completamente\n');
  }

  private async syncAllTables(idEmpresa: number, empresa: string) {
    for (const tabela of this.tabelas) {
      let tentativa = 0;
      let sucesso = false;
      let ultimoErro: any = null;

      while (tentativa < tabela.maxTentativas && !sucesso) {
        tentativa++;
        const dataInicio = new Date();

        try {
          console.log(`  ⏳ [Tentativa ${tentativa}/${tabela.maxTentativas}] Sincronizando ${tabela.nome}...`);
          console.log(`  📋 ID Empresa: ${idEmpresa}, Empresa: ${empresa}`);

          // Chamar a função de sincronização diretamente
          const resultado = await tabela.funcaoSync(idEmpresa, empresa);

          if (resultado && resultado.success) {
            sucesso = true;
            const dataFim = new Date();
            const duracao = dataFim.getTime() - dataInicio.getTime();

            console.log(`  ✓ ${tabela.nome} sincronizado com sucesso`);
            console.log(`    📊 Registros: ${resultado.totalRegistros || 0}`);
            console.log(`    ➕ Inseridos: ${resultado.registrosInseridos || 0}`);
            console.log(`    🔄 Atualizados: ${resultado.registrosAtualizados || 0}`);
            console.log(`    🗑️  Deletados: ${resultado.registrosDeletados || 0}`);
            console.log(`    ⏱️  Duração: ${duracao}ms\n`);
          } else {
            const mensagemErro = resultado?.erro || resultado?.error || 'Sincronização retornou success: false';
            throw new Error(mensagemErro);
          }
        } catch (error: any) {
          ultimoErro = error;
          const dataFim = new Date();
          const duracao = dataFim.getTime() - dataInicio.getTime();

          console.error(`  ✗ Falha na tentativa ${tentativa}/${tabela.maxTentativas}: ${error.message}`);
          console.error(`  📝 Tipo do erro:`, error.constructor.name);
          console.error(`  📝 Stack trace:`, error.stack);

          // Salvar log de erro para cada tentativa
          try {
            await salvarLogSincronizacao({
              ID_SISTEMA: idEmpresa,
              EMPRESA: empresa,
              TABELA: tabela.nome.toUpperCase().replace(/ /g, '_'),
              STATUS: 'FALHA',
              TOTAL_REGISTROS: 0,
              REGISTROS_INSERIDOS: 0,
              REGISTROS_ATUALIZADOS: 0,
              REGISTROS_DELETADOS: 0,
              DURACAO_MS: duracao,
              MENSAGEM_ERRO: `Tentativa ${tentativa}/${tabela.maxTentativas}: ${error.message}`,
              DATA_INICIO: dataInicio,
              DATA_FIM: dataFim
            });
          } catch (logError) {
            console.error('  ⚠️  Erro ao salvar log:', logError);
          }

          // Aguardar antes da próxima tentativa (se não for a última)
          if (tentativa < tabela.maxTentativas) {
            const waitTime = tentativa * 2000; // Espera progressiva: 2s, 4s, 6s
            console.log(`  ⏸️  Aguardando ${waitTime}ms antes da próxima tentativa...\n`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
          }
        }
      }

      if (!sucesso) {
        console.error(`  ❌ Falha após ${tabela.maxTentativas} tentativas em ${tabela.nome}`);
        console.error(`  📝 Último erro: ${ultimoErro?.message || 'Erro desconhecido'}`);
        console.log(`  ➡️  Pulando para a próxima tabela...\n`);
      }
    }
  }

  getQueueStatus() {
    return {
      queueLength: this.queue.length,
      isProcessing: this.isProcessing,
      isSyncRunning: this.isSyncRunning,
      contractsInProcessing: Array.from(this.contractsInProcessing),
      queue: this.queue
    };
  }

  // Método para forçar sincronização (usado para testes ou sincronização manual)
  async forceSyncForContract(idEmpresa: number, empresa: string) {
    // Verificar se já está em processamento
    if (this.contractsInProcessing.has(idEmpresa)) {
      throw new Error('Este contrato já está sendo sincronizado. Aguarde a conclusão.');
    }

    // Verificar se já está na fila
    const jaExiste = this.queue.some(item => item.idEmpresa === idEmpresa);
    if (jaExiste) {
      throw new Error('Este contrato já está na fila de sincronização.');
    }

    // Adicionar à fila
    this.queue.push({
      idEmpresa,
      empresa,
      timestamp: new Date()
    });

    // Processar imediatamente se não estiver processando
    if (!this.isProcessing) {
      this.processQueue();
    }
  }
}

export const syncQueueService = new SyncQueueService();