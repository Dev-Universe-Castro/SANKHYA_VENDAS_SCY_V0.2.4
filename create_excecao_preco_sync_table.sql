
-- Script para criar a tabela AS_EXCECAO_PRECO no Oracle
-- Tabela responsável por armazenar as exceções de preço sincronizadas de cada empresa

-- Criar a tabela AS_EXCECAO_PRECO
CREATE TABLE AS_EXCECAO_PRECO (
    ID_SISTEMA NUMBER NOT NULL,
    CODPROD NUMBER NOT NULL,
    NUTAB NUMBER NOT NULL,
    CODLOCAL NUMBER NOT NULL,
    VLRANT NUMBER(15,2),
    VARIACAO NUMBER(15,2),
    TIPO VARCHAR2(10),
    VLRVENDA NUMBER(15,2),
    CONTROLE VARCHAR2(10),
    DHALTREG TIMESTAMP,
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_excecao_preco PRIMARY KEY (ID_SISTEMA, CODPROD, NUTAB, CODLOCAL),
    CONSTRAINT fk_as_excecao_preco_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_excecao_preco_sistema ON AS_EXCECAO_PRECO(ID_SISTEMA);
CREATE INDEX idx_as_excecao_preco_atual ON AS_EXCECAO_PRECO(SANKHYA_ATUAL);
CREATE INDEX idx_as_excecao_preco_codprod ON AS_EXCECAO_PRECO(CODPROD);
CREATE INDEX idx_as_excecao_preco_nutab ON AS_EXCECAO_PRECO(NUTAB);
CREATE INDEX idx_as_excecao_preco_carga ON AS_EXCECAO_PRECO(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_excecao_preco_atualizacao
BEFORE UPDATE ON AS_EXCECAO_PRECO
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_EXCECAO_PRECO IS 'Tabela de sincronização de exceções de preço do Sankhya por empresa';
COMMENT ON COLUMN AS_EXCECAO_PRECO.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_EXCECAO_PRECO.CODPROD IS 'Código do produto';
COMMENT ON COLUMN AS_EXCECAO_PRECO.NUTAB IS 'Número da tabela de preços';
COMMENT ON COLUMN AS_EXCECAO_PRECO.CODLOCAL IS 'Código do local';
COMMENT ON COLUMN AS_EXCECAO_PRECO.VLRANT IS 'Preço anterior';
COMMENT ON COLUMN AS_EXCECAO_PRECO.VARIACAO IS 'Variação percentual';
COMMENT ON COLUMN AS_EXCECAO_PRECO.TIPO IS 'Tipo de exceção';
COMMENT ON COLUMN AS_EXCECAO_PRECO.VLRVENDA IS 'Preço de venda';
COMMENT ON COLUMN AS_EXCECAO_PRECO.CONTROLE IS 'Controle';
COMMENT ON COLUMN AS_EXCECAO_PRECO.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_EXCECAO_PRECO.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_EXCECAO_PRECO.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas exceções de preço ativas
CREATE OR REPLACE VIEW VW_EXCECAO_PRECO_ATIVOS AS
SELECT 
    ep.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_EXCECAO_PRECO ep
INNER JOIN AD_CONTRATOS c ON ep.ID_SISTEMA = c.ID_EMPRESA
WHERE ep.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_EXCECAO_PRECO_ATIVOS IS 'View de exceções de preço ativas sincronizadas';

COMMIT;
