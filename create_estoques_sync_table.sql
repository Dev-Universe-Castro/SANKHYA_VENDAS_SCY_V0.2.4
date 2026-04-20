-- =====================================================
-- Script de Criação da Tabela AS_ESTOQUES
-- Sincronização de Estoques do Sankhya
-- =====================================================

-- Verificar e dropar tabela se existir
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE AS_ESTOQUES CASCADE CONSTRAINTS';
   DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_ESTOQUES removida com sucesso');
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
      DBMS_OUTPUT.PUT_LINE('ℹ️ Tabela AS_ESTOQUES não existia');
END;
/

-- Criar tabela de estoques
CREATE TABLE AS_ESTOQUES (
    ID_SISTEMA          NUMBER(10)      NOT NULL,
    CODPROD             NUMBER(10)      NOT NULL,
    CODLOCAL            VARCHAR2(10)    NOT NULL,
    ESTOQUE             NUMBER(15,3)    DEFAULT 0,
    ATIVO               CHAR(1)         DEFAULT 'S',
    CONTROLE            VARCHAR2(10),
    SANKHYA_ATUAL       CHAR(1)         DEFAULT 'S',
    DT_ULT_CARGA        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_ESTOQUES PRIMARY KEY (ID_SISTEMA, CODPROD, CODLOCAL),
    CONSTRAINT FK_ESTOQUES_EMPRESA FOREIGN KEY (ID_SISTEMA)
        REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE,
    CONSTRAINT CHK_ESTOQUES_ATIVO CHECK (ATIVO IN ('S', 'N')),
    CONSTRAINT CHK_ESTOQUES_ATUAL CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    CONSTRAINT CHK_ESTOQUES_CONTROLE CHECK (CONTROLE IN ('E', 'L', 'N'))
);

-- Criar índices para otimizar consultas
CREATE INDEX IDX_ESTOQUES_SISTEMA ON AS_ESTOQUES(ID_SISTEMA);
CREATE INDEX IDX_ESTOQUES_CODPROD ON AS_ESTOQUES(CODPROD);
CREATE INDEX IDX_ESTOQUES_CODLOCAL ON AS_ESTOQUES(CODLOCAL);
CREATE INDEX IDX_ESTOQUES_ATUAL ON AS_ESTOQUES(SANKHYA_ATUAL);
CREATE INDEX IDX_ESTOQUES_DT_CARGA ON AS_ESTOQUES(DT_ULT_CARGA);

/

-- Comentários nas colunas
COMMENT ON TABLE AS_ESTOQUES IS 'Tabela de sincronização de estoques do Sankhya por empresa';
COMMENT ON COLUMN AS_ESTOQUES.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_ESTOQUES.CODPROD IS 'Código do produto no Sankhya';
COMMENT ON COLUMN AS_ESTOQUES.CODLOCAL IS 'Código do local de estoque';
COMMENT ON COLUMN AS_ESTOQUES.ESTOQUE IS 'Quantidade em estoque';
COMMENT ON COLUMN AS_ESTOQUES.ATIVO IS 'Indica se o estoque está ativo no Sankhya';
COMMENT ON COLUMN AS_ESTOQUES.CONTROLE IS 'Tipo de controle (E=Estoque, L=Lote, N=Não controlado)';
COMMENT ON COLUMN AS_ESTOQUES.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_ESTOQUES.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_ESTOQUES.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas estoques ativos
CREATE OR REPLACE VIEW VW_ESTOQUES_ATIVOS AS
SELECT
    e.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_ESTOQUES e
INNER JOIN AD_CONTRATOS c ON e.ID_SISTEMA = c.ID_EMPRESA
WHERE e.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_ESTOQUES_ATIVOS IS 'View de estoques ativos no Sankhya e na aplicação';

/

-- Mensagem de sucesso
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_ESTOQUES criada com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ Índices criados com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ View VW_ESTOQUES_ATIVOS criada!');
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/