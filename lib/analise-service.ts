import { redisCacheService } from './redis-cache-service';
import { obterToken } from './sankhya-api';

export interface FiltroAnalise {
  dataInicio: string; // YYYY-MM-DD
  dataFim: string; // YYYY-MM-DD
}

export interface DadosAnalise {
  leads: any[];
  produtosLeads: any[];
  estagiosFunis: any[];
  funis: any[];
  atividades: any[];
  pedidos: any[];
  produtos: any[];
  clientes: any[];
  financeiro: any[];
  filtro: FiltroAnalise;
  timestamp: string;
  totalLeads?: number;
  totalAtividades?: number;
  totalPedidos?: number;
  totalProdutos?: number;
  totalClientes?: number;
  totalFinanceiro?: number;
  valorTotalPedidos?: number;
  valorTotalFinanceiro?: number;
  valorRecebido?: number;
  valorPendente?: number;
  maioresClientes?: any[];
}

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

async function fazerRequisicaoAutenticada(fullUrl: string, method = 'POST', data = {}, retryCount = 0) {
  const MAX_RETRIES = 2;
  const RETRY_DELAY = 1000;

  try {
    const token = await obterToken(retryCount > 0);
    const axios = (await import('axios')).default;

    const config = {
      method: method.toLowerCase(),
      url: fullUrl,
      data: data,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 30000
    };

    const resposta = await axios(config);
    return resposta.data;

  } catch (erro: any) {
    // Se token expirou, forçar renovação e tentar novamente
    if (erro.response && (erro.response.status === 401 || erro.response.status === 403)) {
      console.log("🔄 Token expirado, forçando renovação...");

      if (retryCount < 1) {
        await new Promise(resolve => setTimeout(resolve, 500));
        return fazerRequisicaoAutenticada(fullUrl, method, data, retryCount + 1);
      }

      throw new Error("Sessão expirada. Tente novamente.");
    }

    // Retry para erros de rede ou timeout
    if ((erro.code === 'ECONNABORTED' || erro.code === 'ENOTFOUND' || erro.response?.status >= 500) && retryCount < MAX_RETRIES) {
      console.log(`🔄 Tentando novamente requisição (${retryCount + 1}/${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * (retryCount + 1)));
      return fazerRequisicaoAutenticada(fullUrl, method, data, retryCount + 1);
    }

    const errorDetails = erro.response?.data || erro.message;
    console.error("❌ Erro na requisição Sankhya:", {
      url: fullUrl,
      method,
      error: errorDetails
    });

    throw new Error(erro.response?.data?.statusMessage || erro.message || "Erro na comunicação com o servidor");
  }
}

function formatarDataParaSankhya(dataISO: string): string {
  const [ano, mes, dia] = dataISO.split('-');
  return `${dia}/${mes}/${ano}`;
}

function mapearEntidades(entities: any): any[] {
  if (!entities || !entities.entity) {
    return [];
  }

  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  return entityArray.map((rawEntity: any) => {
    const cleanObject: any = {};

    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }

    return cleanObject;
  });
}

export async function buscarDadosAnalise(
  filtro: FiltroAnalise,
  userId: number,
  isAdmin: boolean = false
): Promise<DadosAnalise> {

  const cacheKey = `analise:${userId}:${filtro.dataInicio}:${filtro.dataFim}`;

  // Verificar cache primeiro
  const cached = await redisCacheService.get<DadosAnalise>(cacheKey);
  if (cached) {
    console.log('✅ Retornando dados de análise do cache');
    return cached;
  }

  console.log('🔍 Buscando dados de análise da API...');

  const dataInicioSankhya = formatarDataParaSankhya(filtro.dataInicio);
  const dataFimSankhya = formatarDataParaSankhya(filtro.dataFim);

  try {
    // 1. Buscar Leads (filtrado por data de criação)
    let criteriaLeads = `DATA_CRIACAO BETWEEN '${dataInicioSankhya}' AND '${dataFimSankhya}' AND ATIVO = 'S'`;
    if (!isAdmin) {
      criteriaLeads += ` AND CODUSUARIO = ${userId}`;
    }

    const leadsPayload = {
      requestBody: {
        dataSet: {
          rootEntity: "AD_LEADS",
          includePresentationFields: "S",
          offsetPage: null,
          disableRowsLimit: true,
          entity: {
            fieldset: {
              list: "CODLEAD, NOME, DESCRICAO, VALOR, CODESTAGIO, DATA_VENCIMENTO, TIPO_TAG, COR_TAG, CODPARC, CODFUNIL, CODUSUARIO, ATIVO, DATA_CRIACAO, DATA_ATUALIZACAO, STATUS_LEAD, MOTIVO_PERDA, DATA_CONCLUSAO"
            }
          },
          criteria: {
            expression: { $: criteriaLeads }
          }
        }
      }
    };

    // 2. Buscar Atividades (filtrado por data OU sem data)
    const atividadesPayload = {
      requestBody: {
        dataSet: {
          rootEntity: "AD_ADLEADSATIVIDADES",
          includePresentationFields: "S",
          offsetPage: null,
          disableRowsLimit: true,
          entity: {
            fieldset: {
              list: "CODATIVIDADE, CODLEAD, TIPO, DESCRICAO, DATA_HORA, DATA_INICIO, DATA_FIM, CODUSUARIO, DADOS_COMPLEMENTARES, COR, ORDEM, ATIVO, STATUS"
            }
          },
          criteria: {
            expression: {
              $: `ATIVO = 'S' AND (DATA_HORA BETWEEN '${dataInicioSankhya}' AND '${dataFimSankhya}' OR DATA_HORA IS NULL)`
            }
          }
        }
      }
    };

    // 3. Buscar Funis
    const funisPayload = {
      requestBody: {
        dataSet: {
          rootEntity: "AD_FUNIS",
          includePresentationFields: "S",
          offsetPage: null,
          disableRowsLimit: true,
          entity: {
            fieldset: {
              list: "CODFUNIL, NOME, DESCRICAO, COR, ATIVO, DATA_CRIACAO, DATA_ATUALIZACAO"
            }
          },
          criteria: {
            expression: { $: "ATIVO = 'S'" }
          }
        }
      }
    };

    // 4. Buscar Estágios de Funis
    const estagiosPayload = {
      requestBody: {
        dataSet: {
          rootEntity: "AD_FUNISESTAGIOS",
          includePresentationFields: "S",
          offsetPage: null,
          disableRowsLimit: true,
          entity: {
            fieldset: {
              list: "CODESTAGIO, CODFUNIL, NOME, ORDEM, COR, ATIVO"
            }
          },
          criteria: {
            expression: { $: "ATIVO = 'S'" }
          }
        }
      }
    };

    // Buscar dados SEQUENCIALMENTE para evitar sobrecarga na API
    console.log('📥 Buscando leads...');
    const leadsRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', leadsPayload).catch(err => {
      console.error('❌ Erro ao buscar leads:', err.message);
      return null;
    });

    console.log('📥 Buscando atividades...');
    const atividadesRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', atividadesPayload).catch(err => {
      console.error('❌ Erro ao buscar atividades:', err.message);
      return null;
    });

    console.log('📥 Buscando funis...');
    const funisRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', funisPayload).catch(err => {
      console.error('❌ Erro ao buscar funis:', err.message);
      return null;
    });

    console.log('📥 Buscando estágios...');
    const estagiosRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', estagiosPayload).catch(err => {
      console.error('❌ Erro ao buscar estágios:', err.message);
      return null;
    });

    console.log('📥 Buscando pedidos...');
    const pedidosPayload = {
      requestBody: {
        dataSet: {
          rootEntity: "CabecalhoNota",
          includePresentationFields: "S",
          offsetPage: null,
          disableRowsLimit: true,
          entity: {
            fieldset: {
              list: "NUNOTA, CODPARC, CODVEND, VLRNOTA, DTNEG"
            }
          },
          criteria: {
            expression: {
              $: `TIPMOV = 'P' AND DTNEG BETWEEN TO_DATE('${dataInicioSankhya}', 'DD/MM/YYYY') AND TO_DATE('${dataFimSankhya}', 'DD/MM/YYYY')`
            }
          },
          ordering: {
            expression: {
              $: "DTNEG DESC, NUNOTA DESC"
            }
          }
        }
      }
    };

    console.log('📤 Payload de pedidos:', JSON.stringify(pedidosPayload, null, 2));

    const pedidosRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', pedidosPayload).catch(err => {
      console.error('❌ Erro ao buscar pedidos:', err.message);
      return null;
    });

    console.log('📥 Buscando produtos...');
    const produtosRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', {
        requestBody: {
          dataSet: {
            rootEntity: "Produto",
            includePresentationFields: "N",
            offsetPage: null,
            disableRowsLimit: true,
            entity: {
              fieldset: {
                list: "CODPROD, DESCRPROD, ATIVO"
              }
            },
            criteria: {
              expression: { $: "ATIVO = 'S'" }
            }
          }
        }
      }).catch(err => {
        console.error('❌ Erro ao buscar produtos:', err.message);
        return null;
      });

    console.log('📥 Buscando clientes...');
    const clientesRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', {
        requestBody: {
          dataSet: {
            rootEntity: "Parceiro",
            includePresentationFields: "N",
            offsetPage: null,
            disableRowsLimit: true,
            entity: {
              fieldset: {
                list: "CODPARC, NOMEPARC, CGC_CPF, CLIENTE, ATIVO"
              }
            },
            criteria: {
              expression: { $: "CLIENTE = 'S' AND ATIVO = 'S'" }
            }
          }
        }
      }).catch(err => {
        console.error('❌ Erro ao buscar clientes:', err.message);
        return null;
      });

    console.log('📦 Respostas recebidas:', {
      leads: !!leadsRes?.responseBody?.entities,
      atividades: !!atividadesRes?.responseBody?.entities,
      funis: !!funisRes?.responseBody?.entities,
      estagios: !!estagiosRes?.responseBody?.entities,
      pedidos: !!pedidosRes?.responseBody?.entities,
      produtos: !!produtosRes?.responseBody?.entities,
      clientes: !!clientesRes?.responseBody?.entities
    });

    // Log detalhado da resposta de pedidos
    if (pedidosRes?.responseBody?.entities) {
      console.log('📊 Resposta de pedidos recebida:', {
        total: pedidosRes.responseBody.entities.total,
        hasEntity: !!pedidosRes.responseBody.entities.entity,
        metadata: pedidosRes.responseBody.entities.metadata ? 'presente' : 'ausente'
      });

      if (pedidosRes.responseBody.entities.entity) {
        console.log('📋 Estrutura do primeiro pedido:', JSON.stringify(
          Array.isArray(pedidosRes.responseBody.entities.entity)
            ? pedidosRes.responseBody.entities.entity[0]
            : pedidosRes.responseBody.entities.entity,
          null,
          2
        ));
      }
    } else {
      console.log('⚠️ Nenhuma resposta de pedidos ou responseBody vazio');
      console.log('📋 Resposta completa de pedidos:', JSON.stringify(pedidosRes, null, 2));
    }

    const leads = leadsRes?.responseBody?.entities ? mapearEntidades(leadsRes.responseBody.entities) : [];
    const atividades = atividadesRes?.responseBody?.entities ? mapearEntidades(atividadesRes.responseBody.entities) : [];
    const funis = funisRes?.responseBody?.entities ? mapearEntidades(funisRes.responseBody.entities) : [];
    const estagiosFunis = estagiosRes?.responseBody?.entities ? mapearEntidades(estagiosRes.responseBody.entities) : [];
    const pedidos = pedidosRes?.responseBody?.entities ? mapearEntidades(pedidosRes.responseBody.entities) : [];
    const produtos = produtosRes?.responseBody?.entities ? mapearEntidades(produtosRes.responseBody.entities) : [];
    const clientes = clientesRes?.responseBody?.entities ? mapearEntidades(clientesRes.responseBody.entities) : [];

    // Log dos dados mapeados
    console.log('📊 Pedidos mapeados:', pedidos.length > 0 ? pedidos.slice(0, 2) : 'NENHUM PEDIDO');

    console.log('📊 Dados mapeados:', {
      leads: leads.length,
      atividades: atividades.length,
      funis: funis.length,
      estagios: estagiosFunis.length,
      pedidos: pedidos.length,
      produtos: produtos.length,
      clientes: clientes.length
    });

    // 5. Buscar Produtos dos Leads encontrados
    let produtosLeads: any[] = [];
    if (leads.length > 0) {
      const codLeadsStr = leads.map(l => l.CODLEAD).join(',');
      const produtosLeadsPayload = {
        requestBody: {
          dataSet: {
            rootEntity: "AD_ADLEADSPRODUTOS",
            includePresentationFields: "S",
            offsetPage: null,
            disableRowsLimit: true,
            entity: {
              fieldset: {
                list: "CODITEM, CODLEAD, CODPROD, DESCRPROD, QUANTIDADE, VLRUNIT, VLRTOTAL, ATIVO, DATA_INCLUSAO"
              }
            },
            criteria: {
              expression: { $: `CODLEAD IN (${codLeadsStr}) AND ATIVO = 'S'` }
            }
          }
        }
      };

      const produtosLeadsRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', produtosLeadsPayload);
      produtosLeads = produtosLeadsRes?.responseBody?.entities ? mapearEntidades(produtosLeadsRes.responseBody.entities) : [];
    }

    // 6. Buscar Títulos a Receber (financeiro, filtrado por data de vencimento) - Removido conforme solicitado
    // const financeiroPayload = { ... };
    // const financeiroRes = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', financeiroPayload).catch(err => { ... });
    // const financeiro = financeiroRes?.responseBody?.entities ? mapearEntidades(financeiroRes.responseBody.entities) : [];

    const resultado: DadosAnalise = {
      leads,
      produtosLeads,
      estagiosFunis,
      funis,
      atividades,
      pedidos,
      produtos,
      clientes,
      financeiro: [], // Financeiro não é mais buscado
      filtro,
      timestamp: new Date().toISOString()
    };

    // Salvar no cache por 30 minutos
    await redisCacheService.set(cacheKey, resultado, 30 * 60);

    console.log('✅ Dados de análise salvos no cache');

    // O bloco de cálculo de métricas foi atualizado para remover o financeiro
    // e ajustar os logs e retornos de acordo.
    console.log(`📊 Dados completos carregados:`, {
      leads: resultado.leads.length,
      atividades: resultado.atividades.length,
      pedidos: resultado.pedidos.length,
      produtos: resultado.produtos.length,
      clientes: resultado.clientes.length,
      funis: resultado.funis.length,
      estagios: resultado.estagiosFunis.length
    });

    // Calcular métricas
    const valorTotalPedidos = resultado.pedidos.reduce((sum, p) => sum + (parseFloat(p.VLRNOTA) || 0), 0);

    // Criar um mapa de clientes para vincular CODPARC ao nome
    const clientesMap = resultado.clientes.reduce((acc, cliente) => {
      acc[cliente.CODPARC] = cliente.NOMEPARC;
      return acc;
    }, {} as Record<string, string>);

    // Agrupar pedidos por cliente para análise de maiores clientes
    const pedidosPorCliente = resultado.pedidos.reduce((acc, pedido) => {
      const codParc = pedido.CODPARC;
      if (!acc[codParc]) {
        acc[codParc] = {
          CODPARC: codParc,
          NOMEPARC: clientesMap[codParc] || 'Cliente não identificado',
          totalPedidos: 0,
          valorTotal: 0,
          pedidos: []
        };
      }
      acc[codParc].totalPedidos += 1;
      acc[codParc].valorTotal += (parseFloat(pedido.VLRNOTA) || 0);
      acc[codParc].pedidos.push({
        numero: pedido.NUNOTA,
        valor: parseFloat(pedido.VLRNOTA) || 0,
        data: pedido.DTNEG
      });
      return acc;
    }, {} as Record<string, any>);

    // Ordenar clientes por valor total e pegar os 20 maiores
    const maioresClientes = Object.values(pedidosPorCliente)
      .sort((a: any, b: any) => b.valorTotal - a.valorTotal)
      .slice(0, 20);

    console.log('🏆 Maiores clientes calculados:', maioresClientes.length);

    return {
      leads: resultado.leads,
      produtosLeads: resultado.produtosLeads,
      atividades: resultado.atividades,
      pedidos: resultado.pedidos,
      produtos: resultado.produtos,
      clientes: resultado.clientes,
      financeiro: [],
      funis: resultado.funis,
      estagiosFunis: resultado.estagiosFunis,
      timestamp: new Date().toISOString(),
      filtro,
      // Métricas calculadas
      totalLeads: resultado.leads.length,
      totalAtividades: resultado.atividades.length,
      totalPedidos: resultado.pedidos.length,
      totalProdutos: resultado.produtos.length,
      totalClientes: resultado.clientes.length,
      totalFinanceiro: 0,
      valorTotalPedidos,
      valorTotalFinanceiro: 0,
      valorRecebido: 0,
      valorPendente: 0,
      // Informação sobre os maiores clientes (pré-calculado)
      maioresClientes: maioresClientes.map(c => ({
        codigo: c.CODPARC,
        nome: c.NOMEPARC,
        totalPedidos: c.totalPedidos,
        valorTotal: c.valorTotal,
        ticketMedio: c.valorTotal / c.totalPedidos,
        pedidos: c.pedidos
      }))
    };
  } catch (erro: any) {
    console.error('❌ Erro ao buscar dados de análise:', erro);
    throw erro;
  }
}

// A função analisarDadosComGemini foi atualizada para processar e retornar os maiores clientes.
// Esta é uma função auxiliar que pode ser chamada por outras partes do sistema.
export async function analisarDadosComGemini(userId: number, pergunta: string) {
  try {
    console.log('📊 Iniciando análise com Gemini para usuário:', userId);

    // Buscar dados do usuário
    const usuario = await usersService.getById(userId);
    if (!usuario) {
      throw new Error('Usuário não encontrado');
    }

    // Buscar leads do usuário
    const leads = await leadsService.getByUserId(userId);

    // Buscar pedidos do usuário
    let pedidos: any[] = [];
    const tipoUsuario = usuario.tipo || usuario.role?.toLowerCase();

    if (tipoUsuario === 'administrador') {
      pedidos = await listarPedidos();
    } else if (tipoUsuario === 'gerente' && usuario.codVendedor) {
      pedidos = await listarPedidosPorGerente(usuario.codVendedor.toString());
    } else if (tipoUsuario === 'vendedor' && usuario.codVendedor) {
      pedidos = await listarPedidos(usuario.codVendedor.toString());
    }

    // Agrupar pedidos por cliente para análise de maiores clientes
    const pedidosPorCliente = pedidos.reduce((acc, pedido) => {
      const codParc = pedido.CODPARC;
      if (!acc[codParc]) {
        acc[codParc] = {
          CODPARC: codParc,
          NOMEPARC: pedido.NOMEPARC,
          totalPedidos: 0,
          valorTotal: 0,
          pedidos: []
        };
      }
      acc[codParc].totalPedidos += 1;
      acc[codParc].valorTotal += (pedido.VLRNOTA || 0);
      acc[codParc].pedidos.push({
        numero: pedido.NUNOTA,
        valor: pedido.VLRNOTA,
        data: pedido.DTNEG
      });
      return acc;
    }, {} as Record<string, any>);

    // Ordenar clientes por valor total
    const clientesOrdenados = Object.values(pedidosPorCliente)
      .sort((a: any, b: any) => b.valorTotal - a.valorTotal);

    // Preparar contexto para o Gemini
    const contexto = {
      usuario: {
        nome: usuario.name,
        tipo: tipoUsuario,
        codVendedor: usuario.codVendedor
      },
      estatisticas: {
        totalLeads: leads.length,
        leadsAtivos: leads.filter(l => l.status === 'Ativo').length,
        leadsPerdidos: leads.filter(l => l.status === 'Perdido').length,
        leadsGanhos: leads.filter(l => l.status === 'Ganho').length,
        totalPedidos: pedidos.length,
        valorTotalPedidos: pedidos.reduce((sum, p) => sum + (p.VLRNOTA || 0), 0),
        totalClientes: clientesOrdenados.length
      },
      maioresClientes: clientesOrdenados.slice(0, 20).map(c => ({
        codigo: c.CODPARC,
        nome: c.NOMEPARC,
        totalPedidos: c.totalPedidos,
        valorTotal: c.valorTotal,
        ticketMedio: c.valorTotal / c.totalPedidos
      })),
      leads: leads.map(l => ({
        nome: l.nome,
        status: l.status,
        estagio: l.estagio,
        valor: l.valor,
        dataAbertura: l.dataAbertura,
        dataFechamento: l.dataFechamento,
        origem: l.origem
      })),
      pedidos: pedidos.map(p => ({
        numero: p.NUNOTA,
        cliente: p.NOMEPARC,
        codCliente: p.CODPARC,
        vendedor: p.NOMEVEND,
        valor: p.VLRNOTA,
        data: p.DTNEG
      }))
    };

    // Simula a chamada ao Gemini com o contexto preparado
    // Em um cenário real, você faria uma chamada para a API do Gemini aqui.
    console.log('🧠 Contexto para Gemini:', JSON.stringify(contexto, null, 2));
    console.log('💬 Pergunta para Gemini:', pergunta);

    // Simulação de resposta do Gemini
    const respostaGeminiSimulada = `Com base nos dados fornecidos, seus ${contexto.estatisticas.totalClientes} maiores clientes são:
${contexto.maioresClientes.map((cliente, index) => `${index + 1}. ${cliente.nome} (Código: ${cliente.codigo}) - Valor Total: R$ ${cliente.valorTotal.toFixed(2)}, Ticket Médio: R$ ${cliente.ticketMedio.toFixed(2)}`).join('\n')}
`;

    return {
      resposta: respostaGeminiSimulada,
      dados: contexto // Retorna os dados que foram usados para gerar a resposta
    };

  } catch (erro: any) {
    console.error('❌ Erro ao analisar dados com Gemini:', erro);
    throw erro;
  }
}

// Placeholder para funções externas que seriam importadas ou definidas em outro lugar
// const usersService = { getById: async (id: number) => ({ id, name: 'Usuário Teste', tipo: 'administrador', codVendedor: 123 }) };
// const leadsService = { getByUserId: async (userId: number) => [{ nome: 'Lead 1', status: 'Ativo', estagio: 'Qualificado', valor: 1000, dataAbertura: '2023-01-01', dataFechamento: null, origem: 'Site' }] };
// const listarPedidos = async () => [{ NUNOTA: 1, CODPARC: 100, NOMEPARC: 'Cliente A', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 1500.50, DTNEG: '2023-10-20' }, { NUNOTA: 2, CODPARC: 101, NOMEPARC: 'Cliente B', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 2500.00, DTNEG: '2023-10-21' }, { NUNOTA: 3, CODPARC: 100, NOMEPARC: 'Cliente A', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 500.75, DTNEG: '2023-10-22' }];
// const listarPedidosPorGerente = async (codVendedor: string) => listarPedidos(); // Simulação

// Se estas funções não estiverem definidas em outro lugar, descomente e adapte as linhas acima.
// Ou certifique-se de que elas estão sendo importadas corretamente.
// Exemplo de como poderiam ser importadas:
// import { usersService } from './users-service';
// import { leadsService } from './leads-service';
// import { listarPedidos, listarPedidosPorGerente } from './pedidos-service';

// Para fins de demonstração, vamos definir stubs simples aqui se não estiverem importadas.
const usersService = { getById: async (id: number) => ({ id, name: 'Usuário Teste', tipo: 'administrador', codVendedor: 123 }) };
const leadsService = { getByUserId: async (userId: number) => [{ nome: 'Lead 1', status: 'Ativo', estagio: 'Qualificado', valor: 1000, dataAbertura: '2023-01-01', dataFechamento: null, origem: 'Site' }] };
const listarPedidos = async () => [{ NUNOTA: 1, CODPARC: 100, NOMEPARC: 'Cliente A', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 1500.50, DTNEG: '2023-10-20' }, { NUNOTA: 2, CODPARC: 101, NOMEPARC: 'Cliente B', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 2500.00, DTNEG: '2023-10-21' }, { NUNOTA: 3, CODPARC: 100, NOMEPARC: 'Cliente A', CODVEND: 1, NOMEVEND: 'Vendedor 1', VLRNOTA: 500.75, DTNEG: '2023-10-22' }];
const listarPedidosPorGerente = async (codVendedor: string) => listarPedidos(); // Simulação