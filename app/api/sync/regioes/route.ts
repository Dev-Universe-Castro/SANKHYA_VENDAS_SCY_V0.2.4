
import { NextRequest, NextResponse } from 'next/server';
import {
    sincronizarRegioesPorEmpresa,
    sincronizarRegioesParcial,
    sincronizarTodasRegioes,
    obterEstatisticasSincronizacao,
    listarRegioes
} from '@/lib/sync-regioes-service';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const idSistema = searchParams.get('idSistema');
        const list = searchParams.get('list');

        if (list === 'true') {
            const data = await listarRegioes(idSistema ? Number(idSistema) : undefined);
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
            { error: error.message || 'Erro ao buscar estatísticas de regiões' },
            { status: 500 }
        );
    }
}

export async function POST(request: NextRequest) {
    try {
        const { searchParams } = new URL(request.url);
        const idSistemaParam = searchParams.get('idSistema');
        const empresaParam = searchParams.get('empresa');
        const typeParam = searchParams.get('type');
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
        const type = typeParam || body.type || 'total';
        const syncAll = syncAllParam === 'true' || body.syncAll;

        if (syncAll) {
            // Sincronizar todas as regiões
            sincronizarTodasRegioes().catch(console.error);

            return NextResponse.json({
                message: 'Sincronização de todas as regiões iniciada em background'
            });
        }

        if (!idSistema || !empresa) {
            return NextResponse.json(
                { error: 'idSistema e empresa são obrigatórios para sincronização individual' },
                { status: 400 }
            );
        }

        // Sincronizar apenas uma empresa
        let resultado;
        if (type === 'partial') {
            resultado = await sincronizarRegioesParcial(Number(idSistema), empresa);
        } else {
            resultado = await sincronizarRegioesPorEmpresa(Number(idSistema), empresa);
        }

        return NextResponse.json(resultado);

    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao sincronizar regiões' },
            { status: 500 }
        );
    }
}
