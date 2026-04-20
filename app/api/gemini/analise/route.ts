
import { NextRequest } from 'next/server';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { cookies } from 'next/headers';
import { buscarDadosAnalise, FiltroAnalise } from '@/lib/analise-service';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');

const SYSTEM_PROMPT = `Você é um Assistente de Análise de Dados especializado em gerar visualizações inteligentes.

SEU PAPEL:
- Analisar dados de vendas, leads, produtos e clientes
- Gerar widgets de visualização (cards, gráficos, tabelas) baseados nos dados
- Retornar SEMPRE um JSON estruturado no formato especificado
- Trabalhar com dados temporais e séries históricas

FORMATO DE RESPOSTA OBRIGATÓRIO:
Você DEVE retornar um JSON válido com a seguinte estrutura:

{
  "widgets": [
    {
      "tipo": "explicacao",
      "titulo": "Análise Realizada",
      "dados": {
        "texto": "Analisei os dados de vendas dos últimos 6 meses e identifiquei os top 5 produtos. A análise mostra um crescimento de 15% no período."
      }
    },
    {
      "tipo": "card",
      "titulo": "Total de Vendas",
      "dados": {
        "valor": "R$ 150.000",
        "variacao": "+15%",
        "subtitulo": "vs mês anterior"
      }
    },
    {
      "tipo": "grafico_linha",
      "titulo": "Evolução Mensal de Vendas",
      "dados": {
        "labels": ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun"],
        "values": [25000, 28000, 32000, 30000, 35000, 40000]
      },
      "metadados": {
        "formatoMonetario": true
      }
    }
  ]
}

TIPOS DE WIDGETS DISPONÍVEIS:

1. explicacao: OBRIGATÓRIO como primeiro widget - explica o que foi analisado
   - texto: Descrição clara da análise realizada

2. card: Para métricas principais
   - valor: Valor principal (use formatação R$ para valores monetários)
   - variacao: Percentual de mudança (ex: "+15%", "-5%")
   - subtitulo: Contexto adicional

3. grafico_barras: Para comparações
   - labels: Array de rótulos
   - values: Array de valores
   - metadados.formatoMonetario: true (para valores em R$)

4. grafico_linha: Para tendências temporais (use para dados com tempo)
   - labels: Array de períodos (ex: meses, dias, anos)
   - values: Array de valores correspondentes
   - metadados.formatoMonetario: true (para valores em R$)

5. grafico_area: Para visualizar volume ao longo do tempo
   - labels: Array de períodos
   - values: Array de valores
   - metadados.formatoMonetario: true (para valores em R$)

6. grafico_pizza: Para distribuições percentuais
   - labels: Array de categorias
   - values: Array de valores

7. grafico_scatter: Para correlações entre variáveis
   - pontos: Array de objetos {x, y, nome}
   - labelX: Rótulo do eixo X
   - labelY: Rótulo do eixo Y

8. grafico_radar: Para comparar múltiplas métricas
   - labels: Array de dimensões
   - values: Array de valores (0-100)

9. tabela: Para dados detalhados
   - colunas: Array de nomes das colunas
   - linhas: Array de arrays com dados

REGRAS IMPORTANTES:
1. O PRIMEIRO widget SEMPRE deve ser do tipo "explicacao" descrevendo a análise
2. SEMPRE retorne JSON válido, nunca texto livre
3. Use gráficos de linha/área para dados temporais (vendas por mês, evolução, etc)
4. Use scatter para correlações (ex: preço vs quantidade vendida)
5. Use radar para comparar métricas múltiplas (ex: performance de vendedores)
6. Escolha os widgets mais adequados para responder a pergunta
7. Use dados reais fornecidos no contexto
8. Seja visual e informativo
9. Priorize insights acionáveis
10. Organize widgets de forma lógica: explicação → métricas principais → gráficos → detalhes
11. SEMPRE adicione metadados.formatoMonetario: true quando os valores forem monetários (vendas, receita, preço, etc)
12. Valores em cards devem ser formatados como "R$ 150.000,00" quando forem monetários`;

export async function POST(request: NextRequest) {
  try {
    const { prompt, dataInicio, dataFim } = await request.json();

    const cookieStore = await cookies();
    const userCookie = cookieStore.get('user');
    let userId = 0;
    let isAdmin = false;
    
    if (userCookie) {
      try {
        const user = JSON.parse(userCookie.value);
        userId = user.id;
        isAdmin = user.role === 'ADMIN';
      } catch (e) {
        console.error('Erro ao parsear cookie:', e);
      }
    }

    // Definir período padrão (últimos 30 dias) se não fornecido
    const hoje = new Date();
    const filtro: FiltroAnalise = {
      dataFim: dataFim || hoje.toISOString().split('T')[0],
      dataInicio: dataInicio || new Date(hoje.setDate(hoje.getDate() - 30)).toISOString().split('T')[0]
    };

    console.log(`📅 Filtro de análise: ${filtro.dataInicio} a ${filtro.dataFim}`);

    const dadosAnalise = await buscarDadosAnalise(filtro, userId, isAdmin);

    // Usar a MESMA estrutura de contexto do Assistente de Vendas
    const contextPrompt = `CONTEXTO DO SISTEMA (${filtro.dataInicio} a ${filtro.dataFim}):

📊 NÚMEROS EXATOS DO SISTEMA (USE ESTES NÚMEROS, NÃO INVENTE):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
→ LEADS: ${dadosAnalise.totalLeads || dadosAnalise.leads.length}
→ ATIVIDADES: ${dadosAnalise.totalAtividades || dadosAnalise.atividades.length}
→ PEDIDOS: ${dadosAnalise.totalPedidos || dadosAnalise.pedidos.length} (Total: R$ ${(dadosAnalise.valorTotalPedidos || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })})
→ PRODUTOS CADASTRADOS: ${dadosAnalise.totalProdutos || dadosAnalise.produtos.length}
→ CLIENTES: ${dadosAnalise.totalClientes || dadosAnalise.clientes.length}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 FUNIS E ESTÁGIOS:
${dadosAnalise.funis.map((f: any) => {
  const estagios = dadosAnalise.estagiosFunis.filter((e: any) => e.CODFUNIL === f.CODFUNIL);
  const leadsNoFunil = dadosAnalise.leads.filter((l: any) => l.CODFUNIL === f.CODFUNIL);
  return `• ${f.NOME} (${estagios.length} estágios, ${leadsNoFunil.length} leads)
  ${estagios.map((e: any) => {
    const leadsNoEstagio = dadosAnalise.leads.filter((l: any) => l.CODESTAGIO === e.CODESTAGIO);
    return `  - ${e.NOME}: ${leadsNoEstagio.length} leads`;
  }).join('\n')}`;
}).join('\n')}

${dadosAnalise.totalLeads && dadosAnalise.totalLeads > 0 ? `💰 LEADS NO PIPELINE (${dadosAnalise.totalLeads}):
${dadosAnalise.leads.slice(0, 20).map((l: any) => {
  const estagio = dadosAnalise.estagiosFunis.find((e: any) => e.CODESTAGIO === l.CODESTAGIO);
  const funil = dadosAnalise.funis.find((f: any) => f.CODFUNIL === l.CODFUNIL);
  const produtos = dadosAnalise.produtosLeads.filter((p: any) => p.CODLEAD === l.CODLEAD);
  return `• ${l.NOME} - R$ ${(l.VALOR || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
  Status: ${l.STATUS_LEAD || 'EM_ANDAMENTO'}
  Estágio: ${estagio?.NOME || 'Sem estágio'} (Funil: ${funil?.NOME || 'Sem funil'})
  ${produtos.length > 0 ? `Produtos: ${produtos.map((p: any) => p.DESCRPROD).join(', ')}` : ''}`;
}).join('\n\n')}` : ''}

${dadosAnalise.totalAtividades && dadosAnalise.totalAtividades > 0 ? `📋 ATIVIDADES (${dadosAnalise.totalAtividades}):
${dadosAnalise.atividades.slice(0, 20).map((a: any) => {
  const lead = dadosAnalise.leads.find((l: any) => l.CODLEAD === a.CODLEAD);
  const desc = a.DESCRICAO?.split('|')[0] || a.DESCRICAO || 'Sem descrição';
  const status = a.STATUS || 'AGUARDANDO';
  const tipo = a.TIPO || '';

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
  }

  return `• ${desc.substring(0, 60)}
  Tipo: ${tipo} | Status: ${status} | Data: ${dataFormatada}
  ${lead ? `Lead: ${lead.NOME}` : 'Sem lead associado'}`;
}).join('\n\n')}` : ''}

${dadosAnalise.totalPedidos && dadosAnalise.totalPedidos > 0 ? `💵 PEDIDOS DE VENDA FINALIZADOS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL EXATO: ${dadosAnalise.totalPedidos} pedidos
VALOR TOTAL: R$ ${(dadosAnalise.valorTotalPedidos || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

${dadosAnalise.maioresClientes && dadosAnalise.maioresClientes.length > 0 ? `🏆 ANÁLISE DE CLIENTES POR VALOR TOTAL (JÁ CALCULADO):

Os ${dadosAnalise.maioresClientes.length} principais clientes por valor total:

${dadosAnalise.maioresClientes.slice(0, 20).map((c: any, idx: number) => `
${idx + 1}º) ${c.nome} (Código: ${c.codigo})
   • Total de Pedidos: ${c.totalPedidos}
   • Valor Total: R$ ${(c.valorTotal || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
   • Ticket Médio: R$ ${(c.ticketMedio || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`).join('\n')}` : 'Nenhum cliente com pedidos'}` : 'Nenhum pedido de venda registrado no período.'}

${dadosAnalise.totalProdutos && dadosAnalise.totalProdutos > 0 ? `📦 CATÁLOGO DE PRODUTOS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL: ${dadosAnalise.totalProdutos} produtos cadastrados
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Exemplos: ${dadosAnalise.produtos.slice(0, 5).map((p: any) => p.DESCRPROD).join(', ')}...` : ''}

PERGUNTA DO USUÁRIO:
${prompt}

IMPORTANTE: Retorne APENAS o JSON estruturado com os widgets. Não adicione texto explicativo antes ou depois do JSON.`;

    const model = genAI.getGenerativeModel({ model: 'gemini-2.0-flash-exp' });

    const result = await model.generateContent([
      { text: SYSTEM_PROMPT },
      { text: contextPrompt }
    ]);

    const responseText = result.response.text();
    
    // Extrair JSON da resposta (remover markdown se houver)
    let jsonText = responseText.trim();
    if (jsonText.startsWith('```json')) {
      jsonText = jsonText.replace(/```json\n?/g, '').replace(/```\n?/g, '');
    } else if (jsonText.startsWith('```')) {
      jsonText = jsonText.replace(/```\n?/g, '');
    }

    const parsedResponse = JSON.parse(jsonText);

    return new Response(JSON.stringify(parsedResponse), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Erro na análise Gemini:', error);
    return new Response(JSON.stringify({ 
      error: 'Erro ao processar análise',
      widgets: []
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
