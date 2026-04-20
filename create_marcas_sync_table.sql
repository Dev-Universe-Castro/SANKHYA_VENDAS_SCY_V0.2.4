
-- Script para criar a tabela AS_MARCAS no Oracle
-- Tabela responsável por armazenar as marcas de produtos sincronizadas de cada empresa

-- Criar a tabela AS_MARCAS
CREATE TABLE AS_MARCAS (
    ID_SISTEMA NUMBER NOT NULL,
    CODIGO NUMBER NOT NULL,
    DESCRICAO VARCHAR2(200),
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_marcas PRIMARY KEY (ID_SISTEMA, CODIGO),
    CONSTRAINT fk_as_marcas_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_marcas_sistema ON AS_MARCAS(ID_SISTEMA);
CREATE INDEX idx_as_marcas_atual ON AS_MARCAS(SANKHYA_ATUAL);
CREATE INDEX idx_as_marcas_descr ON AS_MARCAS(DESCRICAO);
CREATE INDEX idx_as_marcas_carga ON AS_MARCAS(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_marcas_atualizacao
BEFORE UPDATE ON AS_MARCAS
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_MARCAS IS 'Tabela de sincronização de marcas de produtos do Sankhya por empresa';
COMMENT ON COLUMN AS_MARCAS.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_MARCAS.CODIGO IS 'Código da marca no Sankhya (CODMARCA)';
COMMENT ON COLUMN AS_MARCAS.DESCRICAO IS 'Descrição da marca';
COMMENT ON COLUMN AS_MARCAS.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_MARCAS.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_MARCAS.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas marcas ativas
CREATE OR REPLACE VIEW VW_MARCAS_ATIVAS AS
SELECT 
    t.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_MARCAS t
INNER JOIN AD_CONTRATOS c ON t.ID_SISTEMA = c.ID_EMPRESA
WHERE t.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_MARCAS_ATIVAS IS 'View de marcas ativas sincronizadas';

COMMIT;
