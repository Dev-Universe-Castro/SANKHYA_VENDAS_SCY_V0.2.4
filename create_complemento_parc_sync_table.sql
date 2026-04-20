-- Script para criar a tabela AS_COMPLEMENTO_PARC no Oracle
-- Tabela responsável por armazenar os complementos dos parceiros sincronizados de cada empresa

-- Criar a tabela AS_COMPLEMENTO_PARC
CREATE TABLE AS_COMPLEMENTO_PARC (
    ID_SISTEMA NUMBER NOT NULL,
    CODPARC NUMBER NOT NULL,
    SUGTIPNEGSAID NUMBER,
    DTALTER TIMESTAMP,
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_complemento_parc PRIMARY KEY (ID_SISTEMA, CODPARC),
    CONSTRAINT fk_as_comp_parc_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_comp_parc_sistema ON AS_COMPLEMENTO_PARC(ID_SISTEMA);
CREATE INDEX idx_as_comp_parc_atual ON AS_COMPLEMENTO_PARC(SANKHYA_ATUAL);
CREATE INDEX idx_as_comp_parc_carga ON AS_COMPLEMENTO_PARC(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_comp_parc_atualizacao
BEFORE UPDATE ON AS_COMPLEMENTO_PARC
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_COMPLEMENTO_PARC IS 'Tabela de sincronização de dados de Complemento Parceiro do Sankhya por empresa';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.CODPARC IS 'Código do parceiro no Sankhya';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.SUGTIPNEGSAID IS 'Código do Tipo de Negociação de Saída Sugerido';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_COMPLEMENTO_PARC.DT_CRIACAO IS 'Data de criação do registro na tabela';

COMMIT;
