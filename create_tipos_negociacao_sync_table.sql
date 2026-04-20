
-- Script para criar a tabela AS_TIPOS_NEGOCIACAO no Oracle
-- Tabela responsável por armazenar os tipos de negociação sincronizados de cada empresa

-- Criar a tabela AS_TIPOS_NEGOCIACAO
CREATE TABLE AS_TIPOS_NEGOCIACAO (
    ID_SISTEMA NUMBER NOT NULL,
    CODTIPVENDA NUMBER NOT NULL,
    DESCRTIPVENDA VARCHAR2(200),
    DHALTER       TIMESTAMP,
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_tipos_negociacao PRIMARY KEY (ID_SISTEMA, CODTIPVENDA),
    CONSTRAINT fk_as_tipos_negociacao_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_tipos_negociacao_sistema ON AS_TIPOS_NEGOCIACAO(ID_SISTEMA);
CREATE INDEX idx_as_tipos_negociacao_atual ON AS_TIPOS_NEGOCIACAO(SANKHYA_ATUAL);
CREATE INDEX idx_as_tipos_negociacao_descr ON AS_TIPOS_NEGOCIACAO(DESCRTIPVENDA);
CREATE INDEX idx_as_tipos_negociacao_carga ON AS_TIPOS_NEGOCIACAO(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_tipos_negociacao_atualizacao
BEFORE UPDATE ON AS_TIPOS_NEGOCIACAO
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_TIPOS_NEGOCIACAO IS 'Tabela de sincronização de tipos de negociação do Sankhya por empresa';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.CODTIPVENDA IS 'Código do tipo de negociação no Sankhya';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.DESCRTIPVENDA IS 'Descrição do tipo de negociação';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_TIPOS_NEGOCIACAO.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas tipos de negociação ativos
CREATE OR REPLACE VIEW VW_TIPOS_NEGOCIACAO_ATIVOS AS
SELECT 
    t.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_TIPOS_NEGOCIACAO t
INNER JOIN AD_CONTRATOS c ON t.ID_SISTEMA = c.ID_EMPRESA
WHERE t.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_TIPOS_NEGOCIACAO_ATIVOS IS 'View de tipos de negociação ativos sincronizados';

COMMIT;
