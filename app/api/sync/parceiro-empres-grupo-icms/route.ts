import { NextRequest, NextResponse } from 'next/server';
import {
    sincronizarParceiroEmpresGrupoIcmsTotal,
    sincronizarTodasEmpresas,
    obterEstatisticasSincronizacao
} from '@/lib/sync-parceiro-empres-grupo-icms-service';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const idSistema = searchParams.get('idSistema');

        if (idSistema) {
            const stats = await obterEstatisticasSincronizacao(Number(idSistema));
            return NextResponse.json(stats);
        }

        const stats = await obterEstatisticasSincronizacao();
        return NextResponse.json(stats);
    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao buscar estatísticas de parceiro-empres-grupo-icms' },
            { status: 500 }
        );
    }
}

export async function POST(request: NextRequest) {
    try {
        const { searchParams } = new URL(request.url);
        const idSistemaParam = searchParams.get('idSistema');
        const empresaParam = searchParams.get('empresa');
        const syncAllParam = searchParams.get('syncAll');

        let body: any = {};
        try {
            const text = await request.text();
            if (text) {
                body = JSON.parse(text);
            }
        } catch (e) {
            // Ignora erro se corpo estiver vazio
        }

        const idSistema = idSistemaParam || body.idSistema;
        const empresa = empresaParam || body.empresa;
        const syncAll = syncAllParam === 'true' || body.syncAll;

        if (syncAll) {
            sincronizarTodasEmpresas().catch(console.error);
            return NextResponse.json({
                message: 'Sincronização de todas as empresas iniciada em background'
            });
        }

        if (!idSistema || !empresa) {
            return NextResponse.json(
                { error: 'idSistema e empresa são obrigatórios para sincronização individual' },
                { status: 400 }
            );
        }

        const resultado = await sincronizarParceiroEmpresGrupoIcmsTotal(idSistema, empresa);
        return NextResponse.json(resultado);

    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao sincronizar parceiro-empres-grupo-icms' },
            { status: 500 }
        );
    }
}
