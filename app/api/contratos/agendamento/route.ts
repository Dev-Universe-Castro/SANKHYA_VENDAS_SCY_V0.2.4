
import { NextResponse } from "next/server";
import { atualizarAgendamentoSync } from "@/lib/oracle-service";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { id, syncAtivo, intervaloMinutos } = body;

    if (!id || typeof syncAtivo !== 'boolean' || !intervaloMinutos) {
      return NextResponse.json(
        { error: "Dados inválidos" },
        { status: 400 }
      );
    }

    if (intervaloMinutos < 1) {
      return NextResponse.json(
        { error: "Intervalo deve ser maior que 0 minutos" },
        { status: 400 }
      );
    }

    await atualizarAgendamentoSync(id, syncAtivo, intervaloMinutos);

    return NextResponse.json({ 
      success: true,
      message: "Agendamento atualizado com sucesso" 
    });
  } catch (error: any) {
    console.error("Erro ao atualizar agendamento:", error);
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
