import axios from 'axios';
import { cryptoService } from './crypto-service';
import { obterToken } from './sankhya-api';
import type { User } from './types';

export type { User };

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";
const URL_SAVE_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DatasetSP.save&outputType=json";

async function fazerRequisicaoAutenticada(fullUrl: string, method = 'POST', data = {}, retryCount = 0) {
  const MAX_RETRIES = 1;

  // SEMPRE forçar busca do token no Redis (não usar cache local)
  const token = await obterToken(retryCount > 0);

  // Log detalhado do token
  console.log("🔑 [users-service] Token obtido:", {
    tokenPreview: token.substring(0, 50) + '...',
    tokenLength: token.length,
    retryCount,
    timestamp: new Date().toISOString()
  });

  try {
    const config = {
      method: method.toLowerCase(),
      url: fullUrl,
      data: data,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      timeout: 15000,
      validateStatus: (status: number) => status < 500
    };

    console.log("🔄 [users-service] Fazendo requisição:", {
      url: fullUrl,
      method,
      dataKeys: Object.keys(data),
      hasAuthHeader: !!config.headers.Authorization
    });
    const resposta = await axios(config);

    // Verificar se a resposta é HTML ao invés de JSON
    if (typeof resposta.data === 'string' && resposta.data.trim().startsWith('<!DOCTYPE')) {
      console.error("❌ API retornou HTML ao invés de JSON:", resposta.data.substring(0, 200));
      throw new Error("A API retornou uma página HTML. Verifique as credenciais e a URL.");
    }

    return resposta.data;

  } catch (erro: any) {
    // Log detalhado do erro
    console.error("❌ [users-service] Erro na requisição:", {
      status: erro.response?.status,
      statusText: erro.response?.statusText,
      url: fullUrl,
      method,
      retryCount,
      data: typeof erro.response?.data === 'string' ? erro.response?.data.substring(0, 200) : erro.response?.data,
      message: erro.message,
      tokenUsado: token.substring(0, 30) + '...'
    });

    // Se token expirou e ainda não tentou novamente, forçar renovação
    if (erro.response && (erro.response.status === 401 || erro.response.status === 403) && retryCount < MAX_RETRIES) {
      console.log("🔄 [users-service] Token expirado (401/403), forçando renovação...");
      await new Promise(resolve => setTimeout(resolve, 1000));
      // Forçar renovação do token
      await obterToken(true);
      console.log("✅ [users-service] Novo token obtido, tentando novamente...");
      return fazerRequisicaoAutenticada(fullUrl, method, data, retryCount + 1);
    }

    throw new Error(`Falha na comunicação com a API Sankhya: ${erro.response?.data?.statusMessage || erro.message}`);
  }
}

function mapearUsuarios(entities: any): User[] {
  if (!entities || !entities.entity) {
    return [];
  }

  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  return entityArray.map((rawEntity: any) => {
    const cleanObject: any = {};

    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];

      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }

    return {
      id: parseInt(cleanObject.CODUSUARIO) || 0,
      name: cleanObject.NOME || '',
      email: cleanObject.EMAIL || '',
      role: cleanObject.FUNCAO || 'Vendedor',
      status: cleanObject.STATUS || 'pendente',
      password: cleanObject.SENHA || '',
      avatar: cleanObject.AVATAR || '',
      codVendedor: cleanObject.CODVEND || null,
      idEmpresa: parseInt(cleanObject.ID_EMPRESA) || 0
    };
  });
}

export const usersService = {
  async getAll(): Promise<User[]> {
    const USUARIOS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_USUARIOSVENDAS",
          "includePresentationFields": "N",
          "offsetPage": "0",
          "limit": "1000",
          "entity": {
            "fieldset": {
              "list": "CODUSUARIO, ID_EMPRESA, NOME, EMAIL, FUNCAO, STATUS, AVATAR, CODVEND, SENHA"
            }
          }
        }
      }
    };

    try {
      console.log("📤 Enviando requisição para buscar usuários:", JSON.stringify(USUARIOS_PAYLOAD, null, 2));

      const respostaCompleta = await fazerRequisicaoAutenticada(
        URL_CONSULTA_SERVICO,
        'POST',
        USUARIOS_PAYLOAD
      );

      console.log("📥 Resposta completa recebida:", JSON.stringify(respostaCompleta, null, 2));

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log("⚠️ Nenhum usuário encontrado");
        return [];
      }

      const usuarios = mapearUsuarios(entities);
      console.log("✅ Usuários mapeados:", usuarios);
      return usuarios;
    } catch (erro: any) {
      console.error("❌ Erro ao buscar usuários:", erro);
      console.error("❌ Detalhes do erro:", {
        message: erro.message,
        response: erro.response?.data,
        status: erro.response?.status
      });
      return [];
    }
  },

  async getPending(): Promise<User[]> {
    const USUARIOS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_USUARIOSVENDAS",
          "includePresentationFields": "N",
          "offsetPage": "0",
          "limit": "1000",
          "entity": {
            "fieldset": {
              "list": "CODUSUARIO, ID_EMPRESA, NOME, EMAIL, FUNCAO, STATUS, AVATAR, CODVEND, SENHA"
            }
          },
          "criteria": {
            "expression": {
              "$": "STATUS = 'pendente'"
            }
          }
        }
      }
    };

    try {
      const respostaCompleta = await fazerRequisicaoAutenticada(
        URL_CONSULTA_SERVICO,
        'POST',
        USUARIOS_PAYLOAD
      );

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        return [];
      }

      return mapearUsuarios(entities);
    } catch (erro) {
      console.error("Erro ao buscar usuários pendentes:", erro);
      return [];
    }
  },

  async getById(id: number): Promise<User | undefined> {
    const USUARIOS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_USUARIOSVENDAS",
          "includePresentationFields": "N",
          "offsetPage": "0",
          "limit": "1",
          "entity": {
            "fieldset": {
              "list": "CODUSUARIO, ID_EMPRESA, NOME, EMAIL, FUNCAO, STATUS, AVATAR, CODVEND, SENHA"
            }
          },
          "criteria": {
            "expression": {
              "$": `CODUSUARIO = ${id}`
            }
          }
        }
      }
    };

    try {
      const respostaCompleta = await fazerRequisicaoAutenticada(
        URL_CONSULTA_SERVICO,
        'POST',
        USUARIOS_PAYLOAD
      );

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        return undefined;
      }

      const usuarios = mapearUsuarios(entities);
      return usuarios[0];
    } catch (erro) {
      console.error("Erro ao buscar usuário por ID:", erro);
      return undefined;
    }
  },

  async register(userData: { name: string; email: string; password: string }): Promise<User> {
    const existingUsers = await this.search(userData.email);
    if (existingUsers.length > 0) {
      throw new Error("Email já cadastrado");
    }

    const hashedPassword = await cryptoService.hashPassword(userData.password);

    const CREATE_PAYLOAD = {
      "serviceName": "DatasetSP.save",
      "requestBody": {
        "entityName": "AD_USUARIOSVENDAS",
        "standAlone": false,
        "fields": ["NOME", "EMAIL", "SENHA", "FUNCAO", "STATUS"],
        "records": [{
          "values": {
            "0": userData.name,
            "1": userData.email,
            "2": hashedPassword,
            "3": "Usuário",
            "4": "pendente"
          }
        }]
      }
    };

    try {
      await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', CREATE_PAYLOAD);

      console.log('✅ Usuário criado com sucesso');

      // Retornar dados básicos do usuário criado
      return {
        id: 0, // ID será atribuído pela API
        name: userData.name,
        email: userData.email,
        role: 'Usuário',
        status: 'pendente',
        avatar: '',
        password: hashedPassword,
        codVendedor: null // Inicializa como null
      };
    } catch (erro: any) {
      throw new Error(`Erro ao registrar usuário: ${erro.message}`);
    }
  },

  async create(userData: Omit<User, "id">): Promise<User> {
    // Garantir que temos uma senha para criar usuário
    if (!userData.password || userData.password.trim() === '') {
      throw new Error("Senha é obrigatória para criar um novo usuário");
    }

    // Se a senha já parece estar hasheada (tem $2a$ ou $2b$), não fazer hash novamente
    const hashedPassword = userData.password.startsWith('$2')
      ? userData.password
      : await cryptoService.hashPassword(userData.password);

    // Garantir que avatar seja uma string vazia se não fornecido
    const avatarUrl = userData.avatar && userData.avatar.trim() !== '' ? userData.avatar : '';

    const CREATE_PAYLOAD = {
      "serviceName": "DatasetSP.save",
      "requestBody": {
        "entityName": "AD_USUARIOSVENDAS",
        "standAlone": false,
        "fields": ["NOME", "EMAIL", "SENHA", "FUNCAO", "STATUS", "AVATAR", "CODVEND"],
        "records": [{
          "values": {
            "0": userData.name,
            "1": userData.email,
            "2": hashedPassword,
            "3": userData.role,
            "4": userData.status,
            "5": avatarUrl,
            "6": userData.codVendedor || null
          }
        }]
      }
    };

    try {
      const response = await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', CREATE_PAYLOAD);
      console.log("✅ Usuário criado na API:", response);

      // Aguardar um momento para o banco indexar
      await new Promise(resolve => setTimeout(resolve, 500));

      // Tentar buscar o usuário criado com retry
      for (let i = 0; i < 3; i++) {
        const newUsers = await this.search(userData.email);
        if (newUsers.length > 0) {
          console.log("✅ Usuário encontrado após criação:", newUsers[0]);
          return newUsers[0];
        }
        // Aguardar antes de tentar novamente
        if (i < 2) {
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      }

      // Se não encontrou, retornar dados mockados com ID temporário
      console.log("⚠️ Usuário criado mas não encontrado na busca, retornando dados básicos");
      return {
        id: Date.now(), // ID temporário
        name: userData.name,
        email: userData.email,
        role: userData.role,
        status: userData.status,
        avatar: userData.avatar || '',
        password: hashedPassword,
        codVendedor: userData.codVendedor || null
      };
    } catch (erro: any) {
      throw new Error(`Erro ao criar usuário: ${erro.message}`);
    }
  },

  async update(id: number, userData: Partial<User>): Promise<User | null> {
    // Buscar dados atuais do usuário
    const currentUser = await this.getById(id);
    if (!currentUser) {
      throw new Error("Usuário não encontrado");
    }

    console.log("🔄 Atualizando usuário:", { id, userData, currentUser });

    // Mesclar dados atuais com as alterações
    const mergedData = {
      name: userData.name !== undefined ? userData.name : currentUser.name,
      email: userData.email !== undefined ? userData.email : currentUser.email,
      role: userData.role !== undefined ? userData.role : currentUser.role,
      status: userData.status !== undefined ? userData.status : currentUser.status,
      avatar: userData.avatar !== undefined ? userData.avatar : currentUser.avatar,
      codVendedor: userData.codVendedor !== undefined ? userData.codVendedor : currentUser.codVendedor // Atualiza CODVEND
    };

    // Garantir que avatar seja uma string vazia se não fornecido ou nulo
    const avatarUrl = (mergedData.avatar && mergedData.avatar.trim() !== '') ? mergedData.avatar : '';

    console.log("📝 Dados mesclados para atualização:", { mergedData, avatarUrl });

    const fields = ["NOME", "EMAIL", "FUNCAO", "STATUS", "AVATAR", "CODVEND"];
    const values: any = {
      "0": mergedData.name,
      "1": mergedData.email,
      "2": mergedData.role,
      "3": mergedData.status,
      "4": avatarUrl,
      "5": mergedData.codVendedor || null
    };

    // Se há nova senha para atualizar, incluir no payload
    if (userData.password && userData.password.trim() !== '') {
      fields.push("SENHA");
      values["7"] = userData.password;
    }

    const UPDATE_PAYLOAD = {
      "serviceName": "DatasetSP.save",
      "requestBody": {
        "entityName": "AD_USUARIOSVENDAS",
        "standAlone": false,
        "fields": fields,
        "records": [{
          "pk": {
            "CODUSUARIO": String(id)
          },
          "values": values
        }]
      }
    };

    try {
      console.log("📤 Enviando atualização:", UPDATE_PAYLOAD);
      await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', UPDATE_PAYLOAD);

      // Aguardar um momento para o banco atualizar
      await new Promise(resolve => setTimeout(resolve, 300));

      const updatedUser = await this.getById(id);
      console.log("✅ Usuário atualizado:", updatedUser);
      return updatedUser || null;
    } catch (erro: any) {
      console.error("❌ Erro ao atualizar usuário:", erro);
      throw new Error(`Erro ao atualizar usuário: ${erro.message}`);
    }
  },

  async approve(id: number): Promise<User | null> {
    return await this.update(id, { status: 'ativo' });
  },

  async block(id: number): Promise<User | null> {
    return await this.update(id, { status: 'bloqueado' });
  },

  async delete(id: number): Promise<boolean> {
    const UPDATE_PAYLOAD = {
      "serviceName": "DatasetSP.save",
      "requestBody": {
        "entityName": "AD_USUARIOSVENDAS",
        "standAlone": false,
        "fields": ["CODUSUARIO", "STATUS"],
        "records": [{
          "pk": {
            "CODUSUARIO": String(id)
          },
          "values": {
            "1": "bloqueado"
          }
        }]
      }
    };

    try {
      await fazerRequisicaoAutenticada(URL_SAVE_SERVICO, 'POST', UPDATE_PAYLOAD);
      return true;
    } catch (erro) {
      return false;
    }
  },

  async search(term: string): Promise<User[]> {
    const USUARIOS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_USUARIOSVENDAS",
          "includePresentationFields": "N",
          "offsetPage": "0",
          "limit": "1000",
          "entity": {
            "fieldset": {
              "list": "CODUSUARIO, ID_EMPRESA, NOME, EMAIL, FUNCAO, STATUS, AVATAR, CODVEND, SENHA"
            }
          },
          "criteria": {
            "expression": {
              "$": `NOME LIKE '%${term.toUpperCase()}%' OR EMAIL LIKE '%${term.toUpperCase()}%' OR FUNCAO LIKE '%${term.toUpperCase()}%'`
            }
          }
        }
      }
    };

    try {
      const respostaCompleta = await fazerRequisicaoAutenticada(
        URL_CONSULTA_SERVICO,
        'POST',
        USUARIOS_PAYLOAD
      );

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        return [];
      }

      return mapearUsuarios(entities);
    } catch (erro) {
      console.error("Erro ao buscar usuários:", erro);
      return [];
    }
  },

  async getByEmail(email: string): Promise<User[]> {
    const USUARIOS_PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "AD_USUARIOSVENDAS",
          "includePresentationFields": "N",
          "offsetPage": "0",
          "limit": "1",
          "entity": {
            "fieldset": {
              "list": "CODUSUARIO, ID_EMPRESA, NOME, EMAIL, FUNCAO, STATUS, AVATAR, CODVEND, SENHA"
            }
          },
          "criteria": {
            "expression": {
              "$": `UPPER(EMAIL) = '${email.toUpperCase()}'`
            }
          }
        }
      }
    };

    try {
      console.log("🔍 Buscando usuário por email:", email);
      console.log("📤 Payload:", JSON.stringify(USUARIOS_PAYLOAD, null, 2));

      const respostaCompleta = await fazerRequisicaoAutenticada(
        URL_CONSULTA_SERVICO,
        'POST',
        USUARIOS_PAYLOAD
      );

      console.log("📥 Resposta completa:", JSON.stringify(respostaCompleta, null, 2));

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log("⚠️ Nenhum usuário encontrado com o email:", email);
        return [];
      }

      const usuarios = mapearUsuarios(entities);
      console.log("✅ Usuário encontrado:", usuarios.length > 0 ? { id: usuarios[0].id, name: usuarios[0].name, email: usuarios[0].email } : 'nenhum');
      return usuarios;
    } catch (erro: any) {
      console.error("❌ Erro ao buscar usuário por email:", erro);
      console.error("❌ Detalhes:", {
        message: erro.message,
        response: erro.response?.data,
        status: erro.response?.status
      });
      return [];
    }
  }
};