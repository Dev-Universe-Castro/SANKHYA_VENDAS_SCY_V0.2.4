import { syncQueueService } from './lib/sync-queue-service';
import oracledb from 'oracledb';

async function testAutoSync() {
    console.log("Iniciando simulação do ciclo de sincronização...");
    try {
        console.log("1. Ligando o master timer da API (start)...");
        syncQueueService.start();
        
        await new Promise(r => setTimeout(r, 2000));
        
        const status = syncQueueService.getQueueStatus();
        console.log("\nStatus da fila atual:", status);
        
        if (status.queueLength > 0 || status.isProcessing || status.contractsInProcessing.length > 0) {
           console.log("✅ Ciclo localizou contratos e começou o processamento sem quebras críticas no start.");
        } else {
           console.log("⚠️ A fila parece vazia ou não está processando. Verifique o banco para existirem contratos para sincronizar.");
        }
        
    } catch (e) {
        console.error("❌ ERRO DURANTE SIMULAÇÃO DE AUTO SYNC:", e);
        process.exit(1);
    }
    
    setTimeout(() => {
        console.log("\nDesligando timer e finalizando teste...");
        syncQueueService.stop();
        process.exit(0);
    }, 15000);
}

testAutoSync();
