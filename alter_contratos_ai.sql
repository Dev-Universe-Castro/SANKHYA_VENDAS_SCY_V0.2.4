-- Script para adicionar campos de IA na tabela AD_CONTRATOS
-- Execute este script no banco de dados Oracle

ALTER TABLE AD_CONTRATOS ADD (
    AI_PROVEDOR VARCHAR2(50) DEFAULT 'Gemini',
    AI_MODELO VARCHAR2(100) DEFAULT 'gemini-2.0-flash',
    AI_CREDENTIAL VARCHAR2(500)
);

-- Comentários nas novas colunas
COMMENT ON COLUMN AD_CONTRATOS.AI_PROVEDOR IS 'Provedor de IA (Gemini, OpenAI, Grok, etc.)';
COMMENT ON COLUMN AD_CONTRATOS.AI_MODELO IS 'Modelo específico da IA';
COMMENT ON COLUMN AD_CONTRATOS.AI_CREDENTIAL IS 'Chave de API ou credencial do provedor de IA';

-- Migrar dados existentes se necessário (Opcional, pois o GEMINI_API_KEY já existe)
-- UPDATE AD_CONTRATOS SET AI_CREDENTIAL = GEMINI_API_KEY WHERE GEMINI_API_KEY IS NOT NULL;

COMMIT;
