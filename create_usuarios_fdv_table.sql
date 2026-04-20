
-- =====================================================
-- Script de Criação da Tabela AD_USUARIOSVENDAS
-- Controle de usuários do aplicativo FDV por empresa
-- =====================================================

-- Verificar e dropar tabela se existir
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE AD_USUARIOSVENDAS CASCADE CONSTRAINTS';
   DBMS_OUTPUT.PUT_LINE('✅ Tabela AD_USUARIOSVENDAS removida com sucesso');
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
      DBMS_OUTPUT.PUT_LINE('ℹ️ Tabela AD_USUARIOSVENDAS não existia');
END;
/

-- Criar tabela de usuários FDV
CREATE TABLE AD_USUARIOSVENDAS (
    CODUSUARIO          NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ID_EMPRESA          NUMBER(10)      NOT NULL,
    NOME                VARCHAR2(100)   NOT NULL,
    EMAIL               VARCHAR2(100)   NOT NULL,
    SENHA               VARCHAR2(255)   NOT NULL,
    FUNCAO              VARCHAR2(50)    DEFAULT 'Vendedor',
    STATUS              VARCHAR2(20)    DEFAULT 'ativo' CHECK (STATUS IN ('ativo', 'pendente', 'bloqueado')),
    AVATAR              VARCHAR2(500),
    DATACRIACAO         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    DATAATUALIZACAO     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CODVEND             NUMBER(10),

    CONSTRAINT FK_USUARIOSVENDAS_EMPRESA FOREIGN KEY (ID_EMPRESA)
        REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE,
    CONSTRAINT UK_USUARIOSVENDAS_EMAIL UNIQUE (ID_EMPRESA, EMAIL)
);

-- Criar índices para otimizar consultas
CREATE INDEX IDX_USUARIOSVENDAS_EMPRESA ON AD_USUARIOSVENDAS(ID_EMPRESA);
CREATE INDEX IDX_USUARIOSVENDAS_EMAIL ON AD_USUARIOSVENDAS(EMAIL);
CREATE INDEX IDX_USUARIOSVENDAS_STATUS ON AD_USUARIOSVENDAS(STATUS);
CREATE INDEX IDX_USUARIOSVENDAS_FUNCAO ON AD_USUARIOSVENDAS(FUNCAO);
CREATE INDEX IDX_USUARIOSVENDAS_CODVEND ON AD_USUARIOSVENDAS(CODVEND);

-- Comentários nas colunas
COMMENT ON TABLE AD_USUARIOSVENDAS IS 'Tabela de usuários do aplicativo FDV segregados por empresa';
COMMENT ON COLUMN AD_USUARIOSVENDAS.CODUSUARIO IS 'Código único do usuário';
COMMENT ON COLUMN AD_USUARIOSVENDAS.ID_EMPRESA IS 'ID da empresa (contrato) a qual o usuário pertence';
COMMENT ON COLUMN AD_USUARIOSVENDAS.NOME IS 'Nome completo do usuário';
COMMENT ON COLUMN AD_USUARIOSVENDAS.EMAIL IS 'E-mail do usuário (único por empresa)';
COMMENT ON COLUMN AD_USUARIOSVENDAS.SENHA IS 'Senha hasheada do usuário';
COMMENT ON COLUMN AD_USUARIOSVENDAS.FUNCAO IS 'Função do usuário (Vendedor, Gerente, etc.)';
COMMENT ON COLUMN AD_USUARIOSVENDAS.STATUS IS 'Status do usuário (ativo, pendente, bloqueado)';
COMMENT ON COLUMN AD_USUARIOSVENDAS.AVATAR IS 'URL da foto do perfil';
COMMENT ON COLUMN AD_USUARIOSVENDAS.DATACRIACAO IS 'Data de criação do registro';
COMMENT ON COLUMN AD_USUARIOSVENDAS.DATAATUALIZACAO IS 'Data da última atualização';
COMMENT ON COLUMN AD_USUARIOSVENDAS.CODVEND IS 'Código do vendedor vinculado no Sankhya';

-- Criar trigger para atualizar DATAATUALIZACAO automaticamente
CREATE OR REPLACE TRIGGER TRG_USUARIOSVENDAS_UPD
BEFORE UPDATE ON AD_USUARIOSVENDAS
FOR EACH ROW
BEGIN
    :NEW.DATAATUALIZACAO := CURRENT_TIMESTAMP;
END;
/

-- Criar view para consultar usuários ativos por empresa
CREATE OR REPLACE VIEW VW_USUARIOSVENDAS_ATIVOS AS
SELECT 
    u.*,
    c.EMPRESA,
    c.CNPJ
FROM AD_USUARIOSVENDAS u
INNER JOIN AD_CONTRATOS c ON u.ID_EMPRESA = c.ID_EMPRESA
WHERE u.STATUS = 'ativo'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_USUARIOSVENDAS_ATIVOS IS 'View de usuários ativos do aplicativo FDV por empresa';

-- Mensagem de sucesso
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('✅ Tabela AD_USUARIOSVENDAS criada com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ Índices criados com sucesso!');
    DBMS_OUTPUT.PUT_LINE('✅ View VW_USUARIOSVENDAS_ATIVOS criada!');
    DBMS_OUTPUT.PUT_LINE('✅ Trigger de atualização criado!');
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/
