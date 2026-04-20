
import { NextRequest, NextResponse } from 'next/server';
import {
    sincronizarEmpresasPorEmpresa,
    sincronizarTodasEmpresas,
    obterEstatisticasSincronizacao,
    listarEmpresas
} from '@/lib/sync-empresas-service';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const idSistema = searchParams.get('idSistema');
        const list = searchParams.get('list');

        if (list === 'true') {
            const data = await listarEmpresas(idSistema ? Number(idSistema) : undefined);
            return NextResponse.json(data);
        }

        if (idSistema) {
            const stats = await obterEstatisticasSincronizacao(Number(idSistema));
            return NextResponse.json(stats);
        }

        const stats = await obterEstatisticasSincronizacao();
        return NextResponse.json(stats);
    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao buscar estatísticas de empresas' },
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
            // Sincronizar todas as empresas
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

        // Sincronizar apenas uma empresa
        const resultado = await sincronizarEmpresasPorEmpresa(Number(idSistema), empresa);

        return NextResponse.json(resultado);

    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao sincronizar empresas' },
            { status: 500 }
        );
    }
}
