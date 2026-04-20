
import { NextResponse } from 'next/server';
import { usuariosFDVService } from '@/lib/usuarios-fdv-service';

export async function GET(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    const usuario = await usuariosFDVService.getById(parseInt(params.id));
    
    if (!usuario) {
      return NextResponse.json(
        { error: 'Usuário não encontrado' },
        { status: 404 }
      );
    }

    return NextResponse.json(usuario);
  } catch (error: any) {
    console.error('❌ Erro ao buscar usuário FDV:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao buscar usuário FDV' },
      { status: 500 }
    );
  }
}

export async function PUT(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    const body = await request.json();
    const usuarioAtualizado = await usuariosFDVService.update(
      parseInt(params.id),
      body
    );
    return NextResponse.json(usuarioAtualizado);
  } catch (error: any) {
    console.error('❌ Erro ao atualizar usuário FDV:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao atualizar usuário FDV' },
      { status: 500 }
    );
  }
}

export async function DELETE(
  request: Request,
  { params }: { params: { id: string } }
) {
  try {
    await usuariosFDVService.delete(parseInt(params.id));
    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('❌ Erro ao deletar usuário FDV:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao deletar usuário FDV' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
