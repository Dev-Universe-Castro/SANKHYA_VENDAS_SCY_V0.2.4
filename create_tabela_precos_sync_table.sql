
-- Script para criar a tabela AS_TABELA_PRECOS no Oracle
-- Tabela responsável por armazenar as tabelas de preços sincronizadas de cada empresa

-- Criar a tabela AS_TABELA_PRECOS
CREATE TABLE AS_TABELA_PRECOS (
    ID_SISTEMA NUMBER NOT NULL,
    NUTAB NUMBER NOT NULL,
    DTVIGOR DATE,
    PERCENTUAL NUMBER(15,2),
    UTILIZADECCUSTO CHAR(1),
    CODTABORIG NUMBER,
    DTALTER DATE,
    CODTAB NUMBER,
    JAPE_ID VARCHAR2(50),
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_tabela_precos PRIMARY KEY (ID_SISTEMA, NUTAB),
    CONSTRAINT fk_as_tabela_precos_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_tabela_precos_sistema ON AS_TABELA_PRECOS(ID_SISTEMA);
CREATE INDEX idx_as_tabela_precos_atual ON AS_TABELA_PRECOS(SANKHYA_ATUAL);
CREATE INDEX idx_as_tabela_precos_codtab ON AS_TABELA_PRECOS(CODTAB);
CREATE INDEX idx_as_tabela_precos_vigor ON AS_TABELA_PRECOS(DTVIGOR);
CREATE INDEX idx_as_tabela_precos_carga ON AS_TABELA_PRECOS(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_tabela_precos_atualizacao
BEFORE UPDATE ON AS_TABELA_PRECOS
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_TABELA_PRECOS IS 'Tabela de sincronização de tabelas de preços do Sankhya por empresa';
COMMENT ON COLUMN AS_TABELA_PRECOS.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_TABELA_PRECOS.NUTAB IS 'Número único da tabela de preços';
COMMENT ON COLUMN AS_TABELA_PRECOS.DTVIGOR IS 'Data de vigência inicial';
COMMENT ON COLUMN AS_TABELA_PRECOS.PERCENTUAL IS 'Percentual de alteração';
COMMENT ON COLUMN AS_TABELA_PRECOS.UTILIZADECCUSTO IS 'Utiliza desconto do custo para cálculo';
COMMENT ON COLUMN AS_TABELA_PRECOS.CODTABORIG IS 'Código da tabela de origem';
COMMENT ON COLUMN AS_TABELA_PRECOS.DTALTER IS 'Data de alteração';
COMMENT ON COLUMN AS_TABELA_PRECOS.CODTAB IS 'Código da tabela';
COMMENT ON COLUMN AS_TABELA_PRECOS.JAPE_ID IS 'Identificador JAPE';
COMMENT ON COLUMN AS_TABELA_PRECOS.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_TABELA_PRECOS.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_TABELA_PRECOS.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas tabelas de preços ativas
CREATE OR REPLACE VIEW VW_TABELA_PRECOS_ATIVOS AS
SELECT 
    tp.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_TABELA_PRECOS tp
INNER JOIN AD_CONTRATOS c ON tp.ID_SISTEMA = c.ID_EMPRESA
WHERE tp.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_TABELA_PRECOS_ATIVOS IS 'View de tabelas de preços ativas sincronizadas';

COMMIT;
