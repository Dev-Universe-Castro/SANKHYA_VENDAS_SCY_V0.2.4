
import { NextResponse } from 'next/server';
import { consultarEstoqueProduto } from '@/lib/produtos-service';
import { addApiLog } from '@/app/api/admin/api-logs/route';

export async function GET(request: Request) {
  const startTime = Date.now();
  
  try {
    const { searchParams } = new URL(request.url);
    const codProd = searchParams.get('codProd');
    const searchLocal = searchParams.get('searchLocal') || '';

    if (!codProd) {
      return NextResponse.json(
        { error: 'Código do produto é obrigatório' },
        { status: 400 }
      );
    }

    // consultarEstoqueProduto já usa fazerRequisicaoAutenticada que busca o token ativo
    const resultado = await consultarEstoqueProduto(codProd, searchLocal);
    const duration = Date.now() - startTime;

    // Log de sucesso
    await addApiLog({
      method: 'GET',
      url: `/api/sankhya/produtos/estoque?codProd=${codProd}`,
      status: 200,
      duration,
      tokenUsed: true
    });

    return NextResponse.json(resultado);
  } catch (error: any) {
    const duration = Date.now() - startTime;
    console.error('Erro ao consultar estoque:', error);

    // Log de erro
    await addApiLog({
      method: 'GET',
      url: '/api/sankhya/produtos/estoque',
      status: 500,
      duration,
      tokenUsed: false,
      error: error.message
    });

    return NextResponse.json(
      { error: error.message || 'Erro ao consultar estoque' },
      { status: 500 }
    );
  }
}
