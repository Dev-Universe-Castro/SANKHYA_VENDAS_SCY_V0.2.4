-- Adicionar coluna UF na tabela AS_CIDADES (conforme solicitado: campo chamado UF que é o Cód. UF Inteiro)
ALTER TABLE AS_CIDADES ADD UF NUMBER;

-- Comentário da coluna
COMMENT ON COLUMN AS_CIDADES.UF IS 'Código da Unidade Federativa (UF) - Inteiro';
