import { NextResponse } from "next/server"
import { deletarContrato } from "@/lib/oracle-service"

export async function POST(request: Request) {
  try {
    const body = await request.json()
    const { ID_EMPRESA } = body

    if (!ID_EMPRESA) {
      return NextResponse.json({ error: "ID_EMPRESA é obrigatório" }, { status: 400 })
    }

    await deletarContrato(ID_EMPRESA)

    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error("Erro ao deletar contrato:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}