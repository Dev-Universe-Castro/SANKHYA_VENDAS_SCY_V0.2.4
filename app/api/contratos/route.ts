import { NextResponse } from "next/server"
import { listarContratos } from "@/lib/oracle-service"

export async function GET() {
  try {
    const contratos = await listarContratos()
    return NextResponse.json(contratos)
  } catch (error: any) {
    console.error("Erro ao buscar contratos:", error)
    return NextResponse.json({ error: error.message }, { status: 500 })
  }
}

export const dynamic = 'force-dynamic'