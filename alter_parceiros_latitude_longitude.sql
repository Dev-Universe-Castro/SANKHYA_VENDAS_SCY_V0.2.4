
-- Aumentar tamanho das colunas LATITUDE e LONGITUDE para acomodar valores maiores
ALTER TABLE AS_PARCEIROS MODIFY (
  LATITUDE VARCHAR2(50),
  LONGITUDE VARCHAR2(50)
);

-- Verificar alteração
DESC AS_PARCEIROS;
