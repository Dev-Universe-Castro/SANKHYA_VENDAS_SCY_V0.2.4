
-- =====================================================
-- Script de Criação da Tabela AS_VENDEDORES
-- Sincronização de Vendedores e Gerentes do Sankhya
-- =====================================================

-- Verificar e dropar tabela se existir
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE AS_VENDEDORES CASCADE CONSTRAINTS';
   DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_VENDEDORES removida com sucesso');
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
      DBMS_OUTPUT.PUT_LINE('ℹ️ Tabela AS_VENDEDORES não existia');
END;
/

-- Criar tabela de vendedores
CREATE TABLE AS_VENDEDORES (
    ID_SISTEMA          NUMBER(10)      NOT NULL,
    CODVEND             NUMBER(10)      NOT NULL,
    APELIDO             VARCHAR2(50),
    ATIVO               CHAR(1)         DEFAULT 'S',
    ATUACOMPRADOR       CHAR(1),
    CODCARGAHOR         NUMBER(10),
    CODCENCUSPAD        NUMBER(10),
    CODEMP              NUMBER(10),
    CODFORM             NUMBER(10),
    CODFUNC             NUMBER(10),
    CODGER              NUMBER(10),
    CODPARC             NUMBER(10),
    CODREG              NUMBER(10),
    CODUSU              NUMBER(10),
    COMCM               CHAR(1),
    COMGER              NUMBER(15,2),
    COMVENDA            NUMBER(15,2),
    DESCMAX             NUMBER(15,2),
    DIACOM              NUMBER(2),
    DTALTER             TIMESTAMP,
    EMAIL               VARCHAR2(100),
    GRUPODESCVEND       VARCHAR2(50),
    GRUPORETENCAO       VARCHAR2(50),
    PARTICMETA          NUMBER(15,2),
    PERCCUSVAR          NUMBER(15,2),
    PROVACRESC          NUMBER(15,2),
    PROVACRESCCAC       NUMBER(15,2),
    RECHREXTRA          CHAR(1),
    SALDODISP           NUMBER(15,2),
    SALDODISPCAC        NUMBER(15,2),
    SENHA               NUMBER(10),
    TIPCALC             CHAR(1),
    TIPFECHCOM          CHAR(1),
    TIPOCERTIF          CHAR(1),
    TIPVALOR            CHAR(1),
    TIPVEND             CHAR(1),
    VLRHORA             NUMBER(15,2),
    SANKHYA_ATUAL       CHAR(1)         DEFAULT 'S',
    DT_ULT_CARGA        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT PK_VENDEDORES PRIMARY KEY (ID_SISTEMA, CODVEND),
    CONSTRAINT FK_VENDEDORES_EMPRESA FOREIGN KEY (ID_SISTEMA)
        REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE,
    CONSTRAINT CHK_VENDEDORES_ATIVO CHECK (ATIVO IN ('S', 'N')),
    CONSTRAINT CHK_VENDEDORES_ATUAL CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    CONSTRAINT CHK_VENDEDORES_TIPVEND CHECK (TIPVEND IN ('G', 'V'))
);

-- Criar índices para otimizar consultas
CREATE INDEX IDX_VENDEDORES_SISTEMA ON AS_VENDEDORES(ID_SISTEMA);
CREATE INDEX IDX_VENDEDORES_CODVEND ON AS_VENDEDORES(CODVEND);
CREATE INDEX IDX_VENDEDORES_TIPVEND ON AS_VENDEDORES(TIPVEND);
CREATE INDEX IDX_VENDEDORES_CODGER ON AS_VENDEDORES(CODGER);
CREATE INDEX IDX_VENDEDORES_ATUAL ON AS_VENDEDORES(SANKHYA_ATUAL);
CREATE INDEX IDX_VENDEDORES_DT_CARGA ON AS_VENDEDORES(DT_ULT_CARGA);

/

-- Comentários nas colunas
COMMENT ON TABLE AS_VENDEDORES IS 'Tabela de sincronização de vendedores e gerentes do Sankhya por empresa';
COMMENT ON COLUMN AS_VENDEDORES.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_VENDEDORES.CODVEND IS 'Código do vendedor/gerente';
COMMENT ON COLUMN AS_VENDEDORES.APELIDO IS 'Apelido do vendedor/gerente';
COMMENT ON COLUMN AS_VENDEDORES.TIPVEND IS 'Tipo (G=Gerente, V=Vendedor)';
COMMENT ON COLUMN AS_VENDEDORES.CODGER IS 'Código do gerente (para vendedores)';
COMMENT ON COLUMN AS_VENDEDORES.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_VENDEDORES.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_VENDEDORES.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas vendedores ativos
CREATE OR REPLACE VIEW VW_VENDEDORES_ATIVOS AS
SELECT
    v.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_VENDEDORES v
INNER JOIN AD_CONTRATOS c ON v.ID_SISTEMA = c.ID_EMPRESA
WHERE v.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_VENDEDORES_ATIVOS IS 'View de vendedores ativos no Sankhya e na aplicação';

/

-- Mensagem de sucesso
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('✅ Tabela AS_VENDEDORES criada com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ Índices criados com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ View VW_VENDEDORES_ATIVOS criada!');
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/
