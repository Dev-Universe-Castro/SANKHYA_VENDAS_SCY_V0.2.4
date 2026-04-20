
-- Script para adicionar coluna IMAGEM na tabela AS_PRODUTOS
-- A coluna armazenará o binário da imagem do produto

ALTER TABLE AS_PRODUTOS ADD (
    IMAGEM BLOB,
    IMAGEM_CONTENT_TYPE VARCHAR2(100),
    IMAGEM_ATUALIZADA_EM TIMESTAMP
);

-- Criar índice para melhorar performance de consultas
CREATE INDEX idx_as_produtos_imagem_atualizada ON AS_PRODUTOS(IMAGEM_ATUALIZADA_EM);

-- Comentários nas colunas
COMMENT ON COLUMN AS_PRODUTOS.IMAGEM IS 'Imagem binária do produto (BLOB)';
COMMENT ON COLUMN AS_PRODUTOS.IMAGEM_CONTENT_TYPE IS 'Tipo MIME da imagem (image/jpeg, image/png, etc)';
COMMENT ON COLUMN AS_PRODUTOS.IMAGEM_ATUALIZADA_EM IS 'Data/hora da última atualização da imagem';

COMMIT;
