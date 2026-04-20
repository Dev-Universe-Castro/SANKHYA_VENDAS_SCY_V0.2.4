/**
 * Este arquivo é executado automaticamente pelo Next.js quando o servidor inicia
 * Ele roda apenas UMA VEZ, antes de qualquer requisição
 * 
 * Documentação: https://nextjs.org/docs/app/building-your-application/optimizing/instrumentation
 */

export async function register() {
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    const { initSuperAdmin } = await import('./lib/init-super-admin')
    await initSuperAdmin()

    // Iniciar serviço de fila de sincronização
    const { syncQueueService } = await import('./lib/sync-queue-service')
    syncQueueService.start()
  }
}