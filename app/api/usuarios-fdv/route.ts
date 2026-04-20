
import { NextResponse } from 'next/server';
import { usuariosFDVService } from '@/lib/usuarios-fdv-service';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idEmpresa = searchParams.get('idEmpresa');
    const termo = searchParams.get('search');

    let usuarios;

    if (termo) {
      usuarios = await usuariosFDVService.search(
        termo,
        idEmpresa ? parseInt(idEmpresa) : undefined
      );
    } else {
      usuarios = await usuariosFDVService.getAll(
        idEmpresa ? parseInt(idEmpresa) : undefined
      );
    }

    return NextResponse.json(usuarios);
  } catch (error: any) {
    console.error('❌ Erro ao consultar usuários FDV:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao consultar usuários FDV' },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const novoUsuario = await usuariosFDVService.create(body);
    return NextResponse.json(novoUsuario, { status: 201 });
  } catch (error: any) {
    console.error('❌ Erro ao criar usuário FDV:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao criar usuário FDV' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
