import { NextRequest } from 'next/server';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { cookies } from 'next/headers';
import { getCacheService } from '@/lib/redis-cache-wrapper';
import { buscarContratoPorId } from '@/lib/oracle-service';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');

// Função helper para fetch com timeout
async function fetchWithTimeout(url: string, options: RequestInit = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeout);
    return response;
  } catch (error) {
    clearTimeout(timeout);
    console.error(`⚠️ Timeout/erro ao buscar ${url}:`, error);
    throw error;
  }
}

// Função para buscar dados do sistema com filtro de data
async function analisarDadosDoSistema(userId: number, userName: string, isAdmin: boolean = false, filtroFrontend?: { dataInicio: string, dataFim: string }) {
  try {
    // Usar filtro do frontend se disponível, senão usar padrão: últimos 90 dias
    let filtro;
    if (filtroFrontend && filtroFrontend.dataInicio && filtroFrontend.dataFim) {
      filtro = filtroFrontend;
    } else {
      const dataFim = new Date();
      const dataInicio = new Date();
      dataInicio.setDate(dataInicio.getDate() - 90);
      filtro = {
        dataInicio: dataInicio.toISOString().split('T')[0],
        dataFim: dataFim.toISOString().split('T')[0]
      };
    }

    console.log('🔍 Buscando dados completos do sistema com filtro de data:', filtro);

    // Importar serviço de análise dinamicamente
    const { buscarDadosAnalise } = await import('@/lib/analise-service');

    // Buscar TODOS os dados de uma vez, com cache inteligente
    const dadosCompletos = await buscarDadosAnalise(filtro, userId, isAdmin);

    console.log(`📊 Dados completos carregados:`, {
      leads: dadosCompletos.leads.length,
      atividades: dadosCompletos.atividades.length,
      pedidos: dadosCompletos.pedidos.length,
      produtos: dadosCompletos.produtos.length,
      clientes: dadosCompletos.clientes.length,
      financeiro: dadosCompletos.financeiro.length,
      funis: dadosCompletos.funis.length,
      estagios: dadosCompletos.estagiosFunis.length
    });

    // Calcular métricas
    const valorTotalPedidos = dadosCompletos.pedidos.reduce((sum, p) => sum + (parseFloat(p.VLRNOTA) || 0), 0);
    const valorTotalFinanceiro = dadosCompletos.financeiro.reduce((sum, f) => sum + (parseFloat(f.VLRDESDOB) || 0), 0);
    const valorRecebido = dadosCompletos.financeiro.reduce((sum, f) => sum + (parseFloat(f.VLRBAIXA) || 0), 0);

    // Calcular maiores clientes
    const pedidosPorCliente = dadosCompletos.pedidos.reduce((acc: any, p: any) => {
      const nomeCliente = p.NOMEPARC || p.Parceiro_NOMEPARC || 'Cliente Desconhecido';
      const codParc = p.CODPARC || 'SEM_CODIGO';
      const key = `${codParc}|${nomeCliente}`;

      if (!acc[key]) {
        acc[key] = {
          codigo: codParc,
          nome: nomeCliente,
          total: 0,
          qtdPedidos: 0,
          pedidos: []
        };
      }
      const valor = parseFloat(p.VLRNOTA) || 0;
      acc[key].total += valor;
      acc[key].qtdPedidos += 1;
      acc[key].pedidos.push({
        nunota: p.NUNOTA,
        valor: valor,
        data: p.DTNEG
      });
      return acc;
    }, {});

    const maioresClientes = Object.values(pedidosPorCliente)
      .sort((a: any, b: any) => b.total - a.total)
      .map((c: any) => ({
        codigo: c.codparc,
        nome: c.nome,
        totalPedidos: c.qtdPedidos,
        valorTotal: c.total,
        ticketMedio: c.total / c.qtdPedidos,
        pedidos: c.pedidos
      }));

    return {
      leads: dadosCompletos.leads,
      produtosLeads: dadosCompletos.produtosLeads,
      atividades: dadosCompletos.atividades,
      pedidos: dadosCompletos.pedidos,
      produtos: dadosCompletos.produtos,
      clientes: dadosCompletos.clientes,
      financeiro: dadosCompletos.financeiro,
      funis: dadosCompletos.funis,
      estagiosFunis: dadosCompletos.estagiosFunis,
      userName,
      filtro,
      // Métricas calculadas
      totalLeads: dadosCompletos.leads.length,
      totalAtividades: dadosCompletos.atividades.length,
      totalPedidos: dadosCompletos.pedidos.length,
      totalProdutos: dadosCompletos.produtos.length,
      totalClientes: dadosCompletos.clientes.length,
      totalFinanceiro: dadosCompletos.financeiro.length,
      valorTotalPedidos,
      valorTotalFinanceiro,
      valorRecebido,
      valorPendente: valorTotalFinanceiro - valorRecebido,
      maioresClientes // Adicionado aqui
    };
  } catch (error) {
    console.error('❌ Erro ao analisar dados do sistema:', error);
    return {
      leads: [],
      produtosLeads: [],
      atividades: [],
      pedidos: [],
      produtos: [],
      clientes: [],
      financeiro: [],
      funis: [],
      estagiosFunis: [],
      userName,
      filtro: { dataInicio: '', dataFim: '' },
      totalLeads: 0,
      totalAtividades: 0,
      totalPedidos: 0,
      totalProdutos: 0,
      totalClientes: 0,
      totalFinanceiro: 0,
      valorTotalPedidos: 0,
      valorTotalFinanceiro: 0,
      valorRecebido: 0,
      valorPendente: 0,
      maioresClientes: [] // Inicializado como array vazio
    };
  }
}

const SYSTEM_PROMPT = `Você é um Assistente de Vendas Inteligente da Sankhya.

SEU PAPEL:
- Ajudar vendedores a gerenciar leads e atividades
- Sugerir próximas ações baseadas no histórico
- Analisar o pipeline de vendas focando em valores e oportunidades
- Fornecer insights sobre leads e atividades

ESTRUTURA DE DADOS DO SISTEMA:
1. FUNIL: Container de estágios de vendas
2. ESTÁGIOS: Etapas dentro de um funil (ex: Leads, Discovery, Demo, Won)
3. LEADS: Oportunidades de venda dentro de cada estágio
4. ATIVIDADES: Ações relacionadas aos leads (ligações, emails, reuniões, etc)
5. PEDIDOS: Pedidos de venda finalizados (valor total por cliente)
6. CLIENTES: Base de clientes do sistema

HIERARQUIA:
Funil → Estágios → Leads → Atividades/Produtos

VOCÊ TEM ACESSO A:
- Leads e seus estágios dentro dos funis
- Atividades registradas (com status: AGUARDANDO, ATRASADO, REALIZADO)
- Produtos vinculados aos leads (itens de interesse de cada lead)
- Base completa de produtos cadastrados no sistema (catálogo)
- Clientes cadastrados (CODPARC, nome, CPF/CNPJ)
- Pedidos de venda finalizados com CODPARC (código do cliente), nome do cliente e valores totais

FOCO PRINCIPAL:
1. **Atividades**: Analise atividades pendentes, atrasadas e sugestões de follow-up
2. **Leads**: Identifique oportunidades prioritárias, leads parados, conversão entre estágios
3. **Pedidos**: Analise valores totais por cliente, ticket médio, tendências de compra
4. **Pipeline**: Entenda a distribuição de leads nos estágios e funis

COMO VOCÊ DEVE RESPONDER:
1. Seja direto e focado em ações de vendas
2. Use APENAS dados reais do sistema - NUNCA invente números ou informações
3. Quando informar quantidades, use EXATAMENTE os números fornecidos no contexto
4. Sugira próximos passos concretos (ligar, email, reunião)
5. Analise tendências no pipeline
6. Identifique leads e atividades que precisam de atenção

REGRA IMPORTANTE: Se o contexto diz "TOTAL: X", você DEVE responder com esse número exato.

EXEMPLOS DE ANÁLISES QUE VOCÊ PODE FAZER:
- "Quais leads têm atividades atrasadas?"
- "Mostre oportunidades prioritárias por valor"
- "Analise a conversão entre estágios do funil"
- "Quais clientes geraram mais pedidos?" → Use CODPARC dos pedidos
- "Quantos pedidos tenho?" → Use o número EXATO de pedidos fornecido
- "Pedidos por cliente" → Agrupe pedidos usando CODPARC e nome do cliente
- "Principais clientes" → Ordene clientes por valor total de pedidos
- "Sugira próximas atividades para leads parados"

REGRA CRÍTICA SOBRE PEDIDOS E CLIENTES:
- Os pedidos SEMPRE incluem CODPARC (código do cliente) e nome do cliente
- Quando perguntarem sobre pedidos, USE os dados fornecidos no contexto
- NUNCA peça dados adicionais se eles já estão no contexto
- O número de pedidos está CLARAMENTE indicado como "TOTAL EXATO: X pedidos"
- A lista de "MAIORES CLIENTES" já está TOTALMENTE CALCULADA e DISPONÍVEL
- Se você ver a seção "🏆 ANÁLISE DE CLIENTES POR VALOR TOTAL", significa que os dados JÁ ESTÃO PRONTOS
- NUNCA responda "preciso dos dados" quando a seção de MAIORES CLIENTES estiver presente

IMPORTANTE: Se o contexto contém a lista de MAIORES CLIENTES, você DEVE usar esses dados diretamente na sua resposta.
Não peça informações que já foram fornecidas no contexto.

Sempre forneça informações baseadas nos dados reais disponíveis no contexto.`;

export async function POST(request: NextRequest) {
  try {
    const { message, history, filtro } = await request.json();

    // Obter usuário autenticado
    const cookieStore = await cookies();
    const userCookie = cookieStore.get('user');
    let userId = 0;
    let userName = 'Usuário';
    let isAdmin = false;

    if (userCookie) {
      try {
        const user = JSON.parse(userCookie.value);
        userId = user.id;
        userName = user.name || 'Usuário';
        isAdmin = user.role === 'admin';

        // Buscar configuração de IA do contrato da empresa
        if (user.idEmpresa) {
          const contrato = await buscarContratoPorId(user.idEmpresa);
          if (contrato) {
            const aiProvedor = contrato.AI_PROVEDOR || 'Gemini';
            const aiModelo = contrato.AI_MODELO || 'gemini-2.0-flash';
            const aiKey = contrato.AI_CREDENTIAL || contrato.GEMINI_API_KEY || process.env.GEMINI_API_KEY || '';

            console.log(`🤖 Usando IA do Contrato: Provedor=${aiProvedor}, Modelo=${aiModelo}`);

            if (aiProvedor.toLowerCase() === 'gemini') {
              const customGenAI = new GoogleGenerativeAI(aiKey);
              const model = customGenAI.getGenerativeModel({
                model: aiModelo,
                generationConfig: {
                  temperature: 0.7,
                  maxOutputTokens: 1500,
                }
              });

              // Iniciar chat com o modelo configurado
              const chat = model.startChat({
                history: chatHistory,
              });

              // Continuar com o processamento do streaming...
              // (Nota: a lógica de streaming abaixo usará este 'model')
            } else {
              // TODO: Implementar OpenAI, Grok, etc.
              console.warn(`⚠️ Provedor ${aiProvedor} ainda não implementado. Usando Gemini padrão.`);
            }
          }
        }
      } catch (e) {
        console.error('Erro ao parsear cookie ou buscar contrato:', e);
      }
    }

    // Fallback para o modelo padrão se nada foi configurado ou encontrado acima
    const model = genAI.getGenerativeModel({
      model: 'gemini-2.5-flash',
      generationConfig: {
        temperature: 0.7,
        maxOutputTokens: 1500,
      }
    });

    // Montar histórico com prompt de sistema
    const chatHistory = [
      {
        role: 'user',
        parts: [{ text: SYSTEM_PROMPT }],
      },
      {
        role: 'model',
        parts: [{ text: 'Entendido! Sou seu Assistente de Vendas no Sankhya CRM. Estou pronto para analisar seus dados e ajudar você a vender mais. Como posso ajudar?' }],
      },
      ...history.map((msg: any) => ({
        role: msg.role === 'assistant' ? 'model' : 'user',
        parts: [{ text: msg.content }],
      }))
    ];

    // Adicionar contexto de dados APENAS no primeiro prompt do usuário
    let messageWithContext = message;
    if (history.length === 0) {
      console.log('🔍 Primeiro prompt detectado - Buscando dados completos do sistema...');
      const dadosSistema = await analisarDadosDoSistema(userId, userName, isAdmin, filtro);

      if (dadosSistema) {
        // Payload focado em VENDAS (leads, atividades, pedidos)
        messageWithContext = `CONTEXTO DO SISTEMA (${dadosSistema.filtro.dataInicio} a ${dadosSistema.filtro.dataFim}):

👤 Usuário: ${dadosSistema.userName}

📊 NÚMEROS EXATOS DO SISTEMA (USE ESTES NÚMEROS, NÃO INVENTE):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
→ LEADS: ${dadosSistema.totalLeads}
→ ATIVIDADES: ${dadosSistema.totalAtividades}
→ PEDIDOS: ${dadosSistema.totalPedidos} (Total: R$ ${(dadosSistema.valorTotalPedidos || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })})
→ PRODUTOS CADASTRADOS: ${dadosSistema.totalProdutos}
→ CLIENTES: ${dadosSistema.totalClientes}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 FUNIS E ESTÁGIOS:
${dadosSistema.funis.map((f: any) => {
          const estagios = dadosSistema.estagiosFunis.filter((e: any) => e.CODFUNIL === f.CODFUNIL);
          const leadsNoFunil = dadosSistema.leads.filter((l: any) => l.CODFUNIL === f.CODFUNIL);
          return `• ${f.NOME} (${estagios.length} estágios, ${leadsNoFunil.length} leads)
  ${estagios.map((e: any) => {
            const leadsNoEstagio = dadosSistema.leads.filter((l: any) => l.CODESTAGIO === e.CODESTAGIO);
            return `  - ${e.NOME}: ${leadsNoEstagio.length} leads`;
          }).join('\n')}`;
        }).join('\n')}

${dadosSistema.totalLeads > 0 ? `💰 LEADS NO PIPELINE (${dadosSistema.totalLeads}):
${dadosSistema.leads.map((l: any) => {
          const estagio = dadosSistema.estagiosFunis.find((e: any) => e.CODESTAGIO === l.CODESTAGIO);
          const funil = dadosSistema.funis.find((f: any) => f.CODFUNIL === l.CODFUNIL);
          const produtos = dadosSistema.produtosLeads.filter((p: any) => p.CODLEAD === l.CODLEAD);
          return `• ${l.NOME} - R$ ${(l.VALOR || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
  Status: ${l.STATUS_LEAD || 'EM_ANDAMENTO'}
  Estágio: ${estagio?.NOME || 'Sem estágio'} (Funil: ${funil?.NOME || 'Sem funil'})
  ${produtos.length > 0 ? `Produtos: ${produtos.map((p: any) => p.DESCRPROD).join(', ')}` : ''}`;
        }).join('\n\n')}` : ''}

${dadosSistema.totalAtividades > 0 ? `📋 ATIVIDADES (${dadosSistema.totalAtividades}):
${dadosSistema.atividades.map((a: any) => {
          const lead = dadosSistema.leads.find((l: any) => l.CODLEAD === a.CODLEAD);
          const desc = a.DESCRICAO?.split('|')[0] || a.DESCRICAO || 'Sem descrição';
          const status = a.STATUS || 'AGUARDANDO';
          const tipo = a.TIPO || '';

          // Formatar data corretamente
          let dataFormatada = 'Sem data';
          if (a.DATA_INICIO) {
            try {
              const data = new Date(a.DATA_INICIO);
              if (!isNaN(data.getTime())) {
                dataFormatada = data.toLocaleDateString('pt-BR', {
                  day: '2-digit',
                  month: '2-digit',
                  year: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit'
                });
              }
            } catch (e) {
              dataFormatada = 'Data inválida';
            }
          } else if (a.DATA_HORA) {
            try {
              const data = new Date(a.DATA_HORA);
              if (!isNaN(data.getTime())) {
                dataFormatada = data.toLocaleDateString('pt-BR', {
                  day: '2-digit',
                  month: '2-digit',
                  year: 'numeric',
                  hour: '2-digit',
                  minute: '2-digit'
                });
              }
            } catch (e) {
              dataFormatada = 'Data inválida';
            }
          }

          return `• ${desc.substring(0, 60)}
  Tipo: ${tipo} | Status: ${status} | Data: ${dataFormatada}
  ${lead ? `Lead: ${lead.NOME}` : 'Sem lead associado'}`;
        }).join('\n\n')}` : ''}

${dadosSistema.totalPedidos > 0 ? `💵 PEDIDOS DE VENDA FINALIZADOS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL EXATO: ${dadosSistema.totalPedidos} pedidos
VALOR TOTAL: R$ ${(dadosSistema.valorTotalPedidos || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

${dadosSistema.maioresClientes && dadosSistema.maioresClientes.length > 0 ? `🏆 ANÁLISE DE CLIENTES POR VALOR TOTAL (JÁ CALCULADO):

VOCÊ JÁ TEM TODOS OS DADOS DOS CLIENTES ABAIXO - NÃO PEÇA MAIS INFORMAÇÕES!

Os ${dadosSistema.maioresClientes.length} principais clientes por valor total de pedidos são:

${dadosSistema.maioresClientes.slice(0, 20).map((c: any, idx: number) => `
${idx + 1}º) ${c.nome} (Código: ${c.codigo})
   • Total de Pedidos: ${c.totalPedidos}
   • Valor Total: R$ ${(c.valorTotal || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
   • Ticket Médio: R$ ${(c.ticketMedio || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
   • Detalhes dos pedidos: ${c.pedidos?.length || 0} pedidos registrados`).join('\n')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INSTRUÇÕES CRÍTICAS:
• Quando perguntarem "quais os maiores clientes", USE a lista acima
• Quando perguntarem "principais clientes", USE a lista acima
• NUNCA peça mais dados sobre clientes - você já tem TUDO acima
• Os dados incluem: CODPARC, nome do cliente, valor total, quantidade de pedidos
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━` : 'Nenhum cliente com pedidos'}

FINANCEIRO - TÍTULOS A RECEBER (${dadosSistema.financeiro?.length || 0} total):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VALOR TOTAL A RECEBER: R$ ${(dadosSistema.valorTotalFinanceiro || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
VALOR RECEBIDO: R$ ${(dadosSistema.valorRecebido || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
VALOR PENDENTE: R$ ${(dadosSistema.valorPendente || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IMPORTANTE PARA RESPONDER:
• Quando perguntarem sobre "quantos pedidos", responda: ${dadosSistema.totalPedidos} pedidos
• Quando perguntarem sobre "principais clientes" ou "maiores clientes", use a lista de MAIORES CLIENTES acima
• Quando perguntarem sobre "pedidos por cliente", analise a lista de MAIORES CLIENTES acima
• Os dados de pedidos incluem CODPARC (código do cliente) e nome do cliente
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━` : 'Nenhum pedido de venda registrado no período.'}

${dadosSistema.totalProdutos > 0 ? `📦 CATÁLOGO DE PRODUTOS (BASE COMPLETA):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL EXATO: ${dadosSistema.totalProdutos} produtos cadastrados no sistema
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Exemplos: ${dadosSistema.produtos.slice(0, 5).map((p: any) => p.DESCRPROD).join(', ')}...

IMPORTANTE: Quando o usuário perguntar sobre produtos na base, responda com ${dadosSistema.totalProdutos} produtos.` : 'Nenhum produto cadastrado no sistema.'}

PERGUNTA DO USUÁRIO:
${message}`;
        console.log('✅ Contexto anexado (leads, atividades, pedidos, hierarquia funil>estágio>lead, maiores clientes)');
      }
    } else {
      console.log('💬 Prompt subsequente - Usando histórico existente');
    }

    // Remover redundância, o 'chat' já foi iniciado acima

    // Usar streaming com contexto
    const result = await chat.sendMessageStream(messageWithContext);

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      async start(controller) {
        try {
          for await (const chunk of result.stream) {
            const text = chunk.text();
            const data = `data: ${JSON.stringify({ text })}\n\n`;
            controller.enqueue(encoder.encode(data));
          }
          controller.enqueue(encoder.encode('data: [DONE]\n\n'));
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
    });
  } catch (error) {
    console.error('Erro no chat Gemini:', error);
    return new Response(JSON.stringify({ error: 'Erro ao processar mensagem' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}