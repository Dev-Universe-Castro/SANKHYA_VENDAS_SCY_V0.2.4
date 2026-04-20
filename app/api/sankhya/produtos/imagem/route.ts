
import { NextResponse } from 'next/server';
import { getOracleConnection } from '@/lib/oracle-service';
import oracledb from 'oracledb';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const codProd = searchParams.get('codProd');
    const idSistema = searchParams.get('idSistema');

    if (!codProd || !idSistema) {
      return NextResponse.json(
        { error: 'Parâmetros codProd e idSistema são obrigatórios' },
        { status: 400 }
      );
    }

    const connection = await getOracleConnection();

    try {
      const result = await connection.execute(
        `SELECT IMAGEM, IMAGEM_CONTENT_TYPE 
         FROM AS_PRODUTOS 
         WHERE ID_SISTEMA = :idSistema 
           AND CODPROD = :codProd 
           AND IMAGEM IS NOT NULL`,
        {
          idSistema: parseInt(idSistema),
          codProd: codProd
        },
        {
          outFormat: oracledb.OUT_FORMAT_OBJECT
        }
      );

      if (!result.rows || result.rows.length === 0) {
        return NextResponse.json(
          { error: 'Imagem não encontrada' },
          { status: 404 }
        );
      }

      const row: any = result.rows[0];
      // O BLOB já vem como buffer binário do Oracle
      const imageBuffer = row.IMAGEM;
      const contentType = row.IMAGEM_CONTENT_TYPE || 'image/jpeg';

      return new NextResponse(imageBuffer, {
        status: 200,
        headers: {
          'Content-Type': contentType,
          'Cache-Control': 'public, max-age=86400, stale-while-revalidate=172800', // 24h cache
          'CDN-Cache-Control': 'public, max-age=86400',
        },
      });

    } finally {
      await connection.close();
    }

  } catch (error: any) {
    console.error('Erro ao buscar imagem do produto:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao buscar imagem do produto' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
