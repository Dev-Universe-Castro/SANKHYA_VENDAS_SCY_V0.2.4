import axios from 'axios';
import { obterToken } from './sankhya-api';

// Serviço de gerenciamento de funis
export interface Funil {
  CODFUNIL: string
  NOME: string
  DESCRICAO: string
  COR: string
  ATIVO: string
  DATA_CRIACAO: string
  DATA_ATUALIZACAO: string
}

export interface EstagioFunil {
  CODESTAGIO: string
  CODFUNIL: string
  NOME: string
  ORDEM: number
  COR: string
  ATIVO: string
}

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";
const URL_SAVE_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json";

import { getSankhyaToken } from './sankhya-token-manager';

async function fazerRequisicaoAutenticada(fullUrl: string, method = 'POST', data = {}, retry = 0) {
  const maxRetries = 2;

  try {
    const token = await getSankhyaToken(retry > 0);

    const config = {
      method: method.toLowerCase(),
      url: fullUrl,
      data: data,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000
    };

    const resposta = await axios(config);
    return resposta.data;

  } catch (erro: any) {
    if (erro.response && (erro.response.status === 401 || erro.response.status === 403)) {
      console.log("🔄 Token expirado, forçando renovação...");

      // Tentar novamente com novo token
      if (retry < maxRetries) {
        return fazerRequisicaoAutenticada(fullUrl, method, data, retry + 1);
      }
    }

    const errorDetails = erro.response?.data || erro.message;
    throw new Error(`Falha na comunicação com a API Sankhya: ${JSON.stringify(errorDetails)}`);
  }
}

function mapearEntidades(entities: any, primaryKey: string): any[] {
  if (!entities || !entities.entity) {
    return [];
  }

  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  return entityArray.map((rawEntity: any) => {
    const cleanObject: any = {};

    if (rawEntity.$) {
      cleanObject[primaryKey] = rawEntity.$[primaryKey] || "";
    }

    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];

      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }

    return cleanObject;
  });
}

// CONSULTAR FUNIS (Admin vê todos, usuários normais veem apenas os permitidos)
export async function consultarFunis(codUsuario?: number | undefined, isAdmin: boolean = false): Promise<Funil[]> {
  console.log('🔍 consultarFunis chamado com:', { codUsuario, isAdmin });
  
  // Admin vê TODOS os funis ativos, independente de permissões
  if (isAdmin) {
    console.log('👑 Usuário é ADMIN - buscando TODOS os funis');
    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_FUNIS",
          "includePresentationFields": "S",
          "offsetPage": "0",
          "entity": {
            "fieldset": {
              "list": "NOME, DESCRICAO, COR, ATIVO, DATA_CRIACAO, DATA_ATUALIZACAO"
            }
          },
          "criteria": {
            "expression": {
              "$": "ATIVO = 'S'"
            }
          }
        }
      }
    };

    try {
      const resposta = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', PAYLOAD);
      console.log('📥 Resposta da busca de funis (Admin):', JSON.stringify(resposta, null, 2));

      if (!resposta?.responseBody?.entities || !resposta.responseBody.entities.entity) {
        console.log('⚠️ Nenhuma entidade retornada para admin');
        return [];
      }

      const funis = mapearEntidades(resposta.responseBody.entities, 'CODFUNIL') as Funil[];
      console.log(`✅ Admin - ${funis.length} funis retornados:`, funis.map(f => f.NOME));
      return funis;
    } catch (erro) {
      console.error("❌ Erro ao consultar funis:", erro);
      return [];
    }
  }

  // Usuários normais veem apenas funis permitidos
  if (!codUsuario) {
    console.log('⚠️ consultarFunis: codUsuario não fornecido para usuário não-admin');
    return [];
  }

  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "AD_FUNISUSUARIOS",
        "includePresentationFields": "S",
        "offsetPage": "0",
        "entity": {
          "fieldset": {
            "list": "CODFUNIL"
          }
        },
        "criteria": {
          "expression": {
            "$": `CODUSUARIO = ${codUsuario} AND ATIVO = 'S'`
          }
        }
      }
    }
  };

  console.log('🔍 Buscando funis permitidos para usuário:', codUsuario);
  console.log('📤 Payload:', JSON.stringify(PAYLOAD, null, 2));

  try {
    const resposta = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', PAYLOAD);
    
    console.log('📥 Resposta de funis permitidos:', JSON.stringify(resposta, null, 2));

    if (!resposta?.responseBody?.entities) {
      console.log('⚠️ Nenhuma entidade retornada para funis permitidos');
      return [];
    }

    const funisPermitidos = mapearEntidades(resposta.responseBody.entities, 'CODFUNILUSUARIO');
    console.log('📋 Funis permitidos mapeados:', funisPermitidos);
    
    const codFunis = funisPermitidos.map((f: any) => f.CODFUNIL).filter(Boolean);
    console.log('🔢 Códigos de funis extraídos:', codFunis);

    if (codFunis.length === 0) {
      console.log('⚠️ Nenhum código de funil encontrado nas permissões');
      return [];
    }

    // Buscar detalhes dos funis permitidos
    const FUNIS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_FUNIS",
          "includePresentationFields": "S",
          "offsetPage": "0",
          "entity": {
            "fieldset": {
              "list": "NOME, DESCRICAO, COR, ATIVO, DATA_CRIACAO, DATA_ATUALIZACAO"
            }
          },
          "criteria": {
            "expression": {
              "$": `CODFUNIL IN (${codFunis.join(',')}) AND ATIVO = 'S'`
            }
          }
        }
      }
    };

    console.log('🔍 Buscando detalhes dos funis:', codFunis);
    console.log('📤 Payload de funis:', JSON.stringify(FUNIS_PAYLOAD, null, 2));

    const respostaFunis = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', FUNIS_PAYLOAD);
    
    console.log('📥 Resposta de detalhes dos funis:', JSON.stringify(respostaFunis, null, 2));

    if (!respostaFunis?.responseBody?.entities) {
      console.log('⚠️ Nenhuma entidade retornada para detalhes dos funis');
      return [];
    }

    const funis = mapearEntidades(respostaFunis.responseBody.entities, 'CODFUNIL') as Funil[];
    console.log('✅ Funis retornados:', funis);
    
    return funis;
  } catch (erro) {
    console.error("❌ Erro ao consultar funis do usuário:", erro);
    return [];
  }
}

// CONSULTAR ESTÁGIOS DE UM FUNIL
export async function consultarEstagiosFunil(codFunil: string): Promise<EstagioFunil[]> {
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "AD_FUNISESTAGIOS",
        "includePresentationFields": "S",
        "offsetPage": "0",
        "entity": {
          "fieldset": {
            "list": "CODFUNIL, NOME, ORDEM, COR, ATIVO"
          }
        },
        "criteria": {
          "expression": {
            "$": `CODFUNIL = ${codFunil} AND ATIVO = 'S'`
          }
        },
        "orderBy": {
          "ORDEM": "ASC"
        }
      }
    }
  };

  try {
    const resposta = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', PAYLOAD);

    if (!resposta?.responseBody?.entities) {
      return [];
    }

    return mapearEntidades(resposta.responseBody.entities, 'CODESTAGIO') as EstagioFunil[];
  } catch (erro) {
    console.error("❌ Erro ao consultar estágios do funil:", erro);
    return [];
  }
}

// SALVAR FUNIL
export async function salvarFunil(funil: Partial<Funil>): Promise<Funil> {
  const isUpdate = !!funil.CODFUNIL;
  const formatarData = (dataISO: string) => {
    const [ano, mes, dia] = dataISO.split('-');
    return `${dia}/${mes}/${ano}`;
  };
  const currentDate = formatarData(new Date().toISOString().split('T')[0]);

  let fields: string[];
  let values: Record<string, any>;
  let record: any;

  if (isUpdate) {
    fields = ["NOME", "DESCRICAO", "COR", "DATA_ATUALIZACAO"];
    values = {
      "0": funil.NOME || "",
      "1": funil.DESCRICAO || "",
      "2": funil.COR || "#3b82f6",
      "3": currentDate
    };
    record = {
      pk: { CODFUNIL: String(funil.CODFUNIL) },
      values: values
    };
  } else {
    fields = ["NOME", "DESCRICAO", "COR", "ATIVO", "DATA_CRIACAO", "DATA_ATUALIZACAO"];
    values = {
      "0": funil.NOME || "",
      "1": funil.DESCRICAO || "",
      "2": funil.COR || "#3b82f6",
      "3": "S",
      "4": currentDate,
      "5": currentDate
    };
    record = { values: values };
  }

  const PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "AD_FUNIS",
      "standAlone": false,
      "fields": fields,
      "records": [record]
    }
  };

  const resposta = await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', PAYLOAD);
  const funis = await consultarFunis();
  return isUpdate ? funis.find(f => f.CODFUNIL === funil.CODFUNIL)! : funis[funis.length - 1];
}

// SALVAR ESTÁGIO
export async function salvarEstagio(estagio: Partial<EstagioFunil>): Promise<EstagioFunil> {
  const isUpdate = !!estagio.CODESTAGIO;

  let fields: string[];
  let values: Record<string, any>;
  let record: any;

  if (isUpdate) {
    fields = ["NOME", "ORDEM", "COR"];
    values = {
      "0": estagio.NOME || "",
      "1": String(estagio.ORDEM || 0),
      "2": estagio.COR || "#3b82f6"
    };
    record = {
      pk: { CODESTAGIO: String(estagio.CODESTAGIO) },
      values: values
    };
  } else {
    fields = ["CODFUNIL", "NOME", "ORDEM", "COR", "ATIVO"];
    values = {
      "0": String(estagio.CODFUNIL || ""),
      "1": estagio.NOME || "",
      "2": String(estagio.ORDEM || 0),
      "3": estagio.COR || "#3b82f6",
      "4": "S"
    };
    record = { values: values };
  }

  const PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "AD_FUNISESTAGIOS",
      "standAlone": false,
      "fields": fields,
      "records": [record]
    }
  };

  console.log('📤 Salvando estágio com payload:', JSON.stringify(PAYLOAD, null, 2));

  const resposta = await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', PAYLOAD);
  console.log('📥 Resposta do save:', JSON.stringify(resposta, null, 2));

  // Aguardar um pouco para garantir que o registro foi processado
  await new Promise(resolve => setTimeout(resolve, 500));

  const estagios = await consultarEstagiosFunil(String(estagio.CODFUNIL));
  console.log('📋 Estágios após salvar:', JSON.stringify(estagios, null, 2));

  if (isUpdate) {
    const estagioAtualizado = estagios.find(e => e.CODESTAGIO === estagio.CODESTAGIO);
    if (!estagioAtualizado) {
      throw new Error('Estágio atualizado não encontrado');
    }
    return estagioAtualizado;
  }

  // Para criação, verificar se existe um código retornado na resposta
  if (resposta?.responseBody?.pk?.CODESTAGIO) {
    const codEstagioNovo = resposta.responseBody.pk.CODESTAGIO;
    const estagioNovo = estagios.find(e => e.CODESTAGIO === String(codEstagioNovo));
    if (estagioNovo) {
      return estagioNovo;
    }
  }

  // Fallback: retornar o último estágio se existir
  if (estagios.length > 0) {
    return estagios[estagios.length - 1];
  }

  throw new Error('Nenhum estágio encontrado após salvar');
}

// DELETAR FUNIL (soft delete)
export async function deletarFunil(codFunil: string): Promise<void> {
  const formatarData = (dataISO: string) => {
    const [ano, mes, dia] = dataISO.split('-');
    return `${dia}/${mes}/${ano}`;
  };
  const currentDate = formatarData(new Date().toISOString().split('T')[0]);

  const PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "AD_FUNIS",
      "standAlone": false,
      "fields": ["ATIVO", "DATA_ATUALIZACAO"],
      "records": [{
        pk: { CODFUNIL: String(codFunil) },
        values: { "0": "N", "1": currentDate }
      }]
    }
  };

  await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', PAYLOAD);
}

// DELETAR ESTÁGIO (soft delete)
export async function deletarEstagio(codEstagio: string): Promise<void> {
  const PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "AD_FUNISESTAGIOS",
      "standAlone": false,
      "fields": ["ATIVO"],
      "records": [{
        pk: { CODESTAGIO: String(codEstagio) },
        values: { "0": "N" }
      }]
    }
  };

  await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', PAYLOAD);
}

// GERENCIAR PERMISSÕES DE FUNIS POR USUÁRIO

export async function consultarFunisUsuario(codUsuario: number): Promise<string[]> {
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "AD_FUNISUSUARIOS",
        "includePresentationFields": "S",
        "offsetPage": "0",
        "entity": {
          "fieldset": {
            "list": "CODFUNIL"
          }
        },
        "criteria": {
          "expression": {
            "$": `CODUSUARIO = ${codUsuario} AND ATIVO = 'S'`
          }
        }
      }
    }
  };

  try {
    const resposta = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', PAYLOAD);

    if (!resposta?.responseBody?.entities) {
      return [];
    }

    const permissoes = mapearEntidades(resposta.responseBody.entities, 'CODFUNILUSUARIO');
    return permissoes.map((p: any) => String(p.CODFUNIL)).filter(Boolean);
  } catch (erro) {
    console.error("❌ Erro ao consultar funis do usuário:", erro);
    return [];
  }
}

export async function atribuirFunilUsuario(codFunil: string, codUsuario: number): Promise<void> {
  const PAYLOAD = {
    "serviceName": "DatasetSP.save",
    "requestBody": {
      "entityName": "AD_FUNISUSUARIOS",
      "standAlone": false,
      "fields": ["CODFUNIL", "CODUSUARIO", "ATIVO"],
      "records": [{
        "values": {
          "0": String(codFunil),
          "1": String(codUsuario),
          "2": "S"
        }
      }]
    }
  };

  await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', PAYLOAD);
}

export async function removerFunilUsuario(codFunil: string, codUsuario: number): Promise<void> {
  // Primeiro, buscar o registro ativo
  const BUSCA_PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "AD_FUNISUSUARIOS",
        "includePresentationFields": "S",
        "offsetPage": "0",
        "entity": {
          "fieldset": {
            "list": "CODFUNILUSUARIO, CODFUNIL, CODUSUARIO"
          }
        },
        "criteria": {
          "expression": {
            "$": `CODFUNIL = ${codFunil} AND CODUSUARIO = ${codUsuario} AND ATIVO = 'S'`
          }
        }
      }
    }
  };

  try {
    const resposta = await fazerRequisicaoAutenticada(URL_CONSULTA_SERVICO, 'POST', BUSCA_PAYLOAD);

    if (!resposta?.responseBody?.entities?.entity) {
      console.log(`ℹ️ Nenhum registro ativo encontrado para CODFUNIL=${codFunil} e CODUSUARIO=${codUsuario}`);
      return;
    }

    const entidades = mapearEntidades(resposta.responseBody.entities, 'CODFUNILUSUARIO');

    // Inativar cada registro encontrado (soft delete)
    for (const entidade of entidades) {
      const INATIVAR_PAYLOAD = {
        "serviceName": "DatasetSP.save",
        "requestBody": {
          "entityName": "AD_FUNISUSUARIOS",
          "standAlone": false,
          "fields": ["ATIVO"],
          "records": [{
            pk: { CODFUNILUSUARIO: String(entidade.CODFUNILUSUARIO) },
            values: { "0": "N" }
          }]
        }
      };

      await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', INATIVAR_PAYLOAD);
      console.log(`✅ Permissão de funil inativada: CODFUNILUSUARIO=${entidade.CODFUNILUSUARIO}`);
    }
  } catch (erro) {
    console.error("❌ Erro ao remover permissão de funil:", erro);
    throw erro;
  }
}