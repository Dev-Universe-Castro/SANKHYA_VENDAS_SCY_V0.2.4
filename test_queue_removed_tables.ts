import { syncQueueService } from './lib/sync-queue-service';

async function main() {
    console.log("Iniciando teste da fila de sincronização...");
    try {
        // Tentando forçar sync para ver se vai travar em alguma tabela removida
        console.log("Adicionando contrato de teste à fila...");
        
        // Mock da funcao processQueue para apenas listar as tabelas que iria sincronizar
        // ou podemos usar a verdadeira para sync real, mas para testar, podemos
        // acessar private field 'tabelas' e ver o que está mapeado
        
        const tabelasMapeadas = (syncQueueService as any).tabelas;
        console.log(`\n📋 Foram encontradas ${tabelasMapeadas.length} tabelas configuradas na fila:`);
        
        let hasRemovedTables = false;
        tabelasMapeadas.forEach((t: any, index: number) => {
            console.log(`  ${index + 1}. ${t.nome} (Rota: ${t.rota})`);
            if (t.nome.toLowerCase().includes('cabeçalho') || t.nome.toLowerCase().includes('item') || t.nome.toLowerCase().includes('nota')) {
                hasRemovedTables = true;
                console.log(`     ⚠️ AVISO: Encontrou tabela suspeita (${t.nome}), que deveria ter sido removida!`);
            }
        });
        
        if (hasRemovedTables) {
            console.error("\n❌ FALHA: Tabelas de notas ainda estão na fila de config!");
            process.exit(1);
        } else {
            console.log("\n✅ SUCESSO: Nenhuma tabela de nota encontrada na configuração da fila de sync.");
        }
    } catch (e) {
        console.error("Erro no teste:", e);
    }
    
    process.exit(0);
}

main();
