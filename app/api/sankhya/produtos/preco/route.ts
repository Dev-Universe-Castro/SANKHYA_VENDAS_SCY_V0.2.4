
import { NextResponse } from 'next/server';
import { addApiLog } from '@/app/api/admin/api-logs/route';
import { obterToken } from '@/lib/sankhya-api';
import axios from 'axios';

export async function GET(request: Request) {
  const startTime = Date.now();
  
  try {
    const { searchParams } = new URL(request.url);
    const codProd = searchParams.get('codProd');

    if (!codProd) {
      return NextResponse.json(
        { error: 'Código do produto é obrigatório' },
        { status: 400 }
      );
    }

    console.log(`🔍 Buscando preço do produto ${codProd}...`);

    // Buscar token ativo do servidor
    const token = await obterToken();
    const url = `https://api.sandbox.sankhya.com.br/v1/precos/produto/${codProd}/tabela/0?pagina=1`;
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 10000
    });
    
    const duration = Date.now() - startTime;

    console.log('📦 Resposta da API de preços:', response.data);

    // Log de sucesso
    await addApiLog({
      method: 'GET',
      url,
      status: response.status,
      duration,
      tokenUsed: true
    });

    // Extrair o preço da resposta
    let preco = 0;
    if (response.data && response.data.produtos && Array.isArray(response.data.produtos) && response.data.produtos.length > 0) {
      const produto = response.data.produtos[0];
      preco = produto.valor || 0;
    }

    console.log(`💰 Preço encontrado: R$ ${preco}`);

    return NextResponse.json({ preco: parseFloat(preco) || 0 });
  } catch (error: any) {
    const duration = Date.now() - startTime;
    console.error('❌ Erro ao buscar preço do produto:', error);
    
    const { searchParams } = new URL(request.url);
    const codProd = searchParams.get('codProd');
    
    // Log de erro
    await addApiLog({
      method: 'GET',
      url: `https://api.sandbox.sankhya.com.br/v1/precos/produto/${codProd}/tabela/0`,
      status: error.response?.status || 500,
      duration,
      tokenUsed: false,
      error: error.message
    });
    
    return NextResponse.json({ preco: 0 });
  }
}
