
-- =====================================================
-- Script de Criação da Tabela AS_TIPOS_OPERACAO
-- Sincronização de Tipos de Operação do Sankhya
-- =====================================================

-- Verificar e dropar tabela se existir
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE AS_TIPOS_OPERACAO CASCADE CONSTRAINTS';
   DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_TIPOS_OPERACAO removida com sucesso');
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
      DBMS_OUTPUT.PUT_LINE('ℹ️ Tabela AS_TIPOS_OPERACAO não existia');
END;
/

-- Criar tabela de tipos de operação
CREATE TABLE AS_TIPOS_OPERACAO (
    ID_SISTEMA          NUMBER(10)      NOT NULL,
    CODTIPOPER          NUMBER(10)      NOT NULL,
    DESCROPER           VARCHAR2(60),
    ATIVO               CHAR(1)         DEFAULT 'S',
    DHALTER             TIMESTAMP,
    SANKHYA_ATUAL       CHAR(1)         DEFAULT 'S',
    DT_ULT_CARGA        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT PK_TIPOS_OPERACAO PRIMARY KEY (ID_SISTEMA, CODTIPOPER),
    CONSTRAINT FK_TIPOS_OPERACAO_EMPRESA FOREIGN KEY (ID_SISTEMA) 
        REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE,
    CONSTRAINT CHK_TIPOS_OPERACAO_ATIVO CHECK (ATIVO IN ('S', 'N')),
    CONSTRAINT CHK_TIPOS_OPERACAO_ATUAL CHECK (SANKHYA_ATUAL IN ('S', 'N'))
);

-- Criar índices para otimizar consultas
CREATE INDEX IDX_TIPOS_OPERACAO_SISTEMA ON AS_TIPOS_OPERACAO(ID_SISTEMA);
CREATE INDEX IDX_TIPOS_OPERACAO_ATIVO ON AS_TIPOS_OPERACAO(SANKHYA_ATUAL);
CREATE INDEX IDX_TIPOS_OPERACAO_DESCROPER ON AS_TIPOS_OPERACAO(DESCROPER);
CREATE INDEX IDX_TIPOS_OPERACAO_DT_CARGA ON AS_TIPOS_OPERACAO(DT_ULT_CARGA);

/

-- Comentários nas colunas
COMMENT ON TABLE AS_TIPOS_OPERACAO IS 'Tabela de sincronização de tipos de operação do Sankhya por empresa';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.CODTIPOPER IS 'Código do tipo de operação no Sankhya';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.DESCROPER IS 'Descrição do tipo de operação';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.ATIVO IS 'Indica se o tipo de operação está ativo no Sankhya';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_TIPOS_OPERACAO.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas tipos de operação ativos
CREATE OR REPLACE VIEW VW_TIPOS_OPERACAO_ATIVOS AS
SELECT 
    t.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_TIPOS_OPERACAO t
INNER JOIN AD_CONTRATOS c ON t.ID_SISTEMA = c.ID_EMPRESA
WHERE t.SANKHYA_ATUAL = 'S'
  AND t.ATIVO = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_TIPOS_OPERACAO_ATIVOS IS 'View de tipos de operação ativos no Sankhya e na aplicação';

/

-- Mensagem de sucesso
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_TIPOS_OPERACAO criada com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ Índices criados com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ View VW_TIPOS_OPERACAO_ATIVOS criada!');
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/
