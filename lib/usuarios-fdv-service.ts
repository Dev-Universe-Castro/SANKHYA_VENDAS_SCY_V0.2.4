
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { cryptoService } from './crypto-service';

export interface UsuarioFDV {
  CODUSUARIO: number;
  ID_EMPRESA: number;
  NOME: string;
  EMAIL: string;
  SENHA?: string;
  FUNCAO: string;
  STATUS: 'ativo' | 'pendente' | 'bloqueado';
  AVATAR?: string;
  DATACRIACAO: Date;
  DATAATUALIZACAO: Date;
  CODVEND?: number;
  EMPRESA?: string;
  CNPJ?: string;
}

export const usuariosFDVService = {
  async getAll(idEmpresa?: number): Promise<UsuarioFDV[]> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      let query = `
        SELECT 
          u.CODUSUARIO,
          u.ID_EMPRESA,
          u.NOME,
          u.EMAIL,
          u.SENHA,
          u.FUNCAO,
          u.STATUS,
          u.AVATAR,
          u.DATACRIACAO,
          u.DATAATUALIZACAO,
          u.CODVEND,
          c.EMPRESA,
          c.CNPJ
        FROM AD_USUARIOSVENDAS u
        INNER JOIN AD_CONTRATOS c ON u.ID_EMPRESA = c.ID_EMPRESA
        WHERE 1=1
      `;

      const binds: any = {};

      if (idEmpresa) {
        query += ` AND u.ID_EMPRESA = :idEmpresa`;
        binds.idEmpresa = idEmpresa;
      }

      query += ` ORDER BY u.NOME`;

      const result = await connection.execute(query, binds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT
      });

      const usuarios = (result.rows as any[]) || [];
      
      // Garantir que SENHA seja string e não null/undefined
      return usuarios.map(u => ({
        ...u,
        SENHA: u.SENHA ? String(u.SENHA) : ''
      }));
    } catch (error) {
      console.error('❌ Erro ao buscar usuários FDV:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async getById(codUsuario: number): Promise<UsuarioFDV | null> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      const result = await connection.execute(
        `
        SELECT 
          u.CODUSUARIO,
          u.ID_EMPRESA,
          u.NOME,
          u.EMAIL,
          u.SENHA,
          u.FUNCAO,
          u.STATUS,
          u.AVATAR,
          u.DATACRIACAO,
          u.DATAATUALIZACAO,
          u.CODVEND,
          c.EMPRESA,
          c.CNPJ
        FROM AD_USUARIOSVENDAS u
        INNER JOIN AD_CONTRATOS c ON u.ID_EMPRESA = c.ID_EMPRESA
        WHERE u.CODUSUARIO = :codUsuario
        `,
        { codUsuario },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const rows = result.rows as any[];
      if (rows && rows.length > 0) {
        const usuario = rows[0];
        return {
          ...usuario,
          SENHA: usuario.SENHA ? String(usuario.SENHA) : ''
        };
      }
      return null;
    } catch (error) {
      console.error('❌ Erro ao buscar usuário FDV por ID:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async getByEmail(email: string, idEmpresa: number): Promise<UsuarioFDV | null> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      const result = await connection.execute(
        `
        SELECT 
          u.CODUSUARIO,
          u.ID_EMPRESA,
          u.NOME,
          u.EMAIL,
          u.SENHA,
          u.FUNCAO,
          u.STATUS,
          u.AVATAR,
          u.DATACRIACAO,
          u.DATAATUALIZACAO,
          u.CODVEND,
          c.EMPRESA,
          c.CNPJ
        FROM AD_USUARIOSVENDAS u
        INNER JOIN AD_CONTRATOS c ON u.ID_EMPRESA = c.ID_EMPRESA
        WHERE UPPER(u.EMAIL) = UPPER(:email)
          AND u.ID_EMPRESA = :idEmpresa
        `,
        { email, idEmpresa },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const rows = result.rows as any[];
      if (rows && rows.length > 0) {
        const usuario = rows[0];
        return {
          ...usuario,
          SENHA: usuario.SENHA ? String(usuario.SENHA) : ''
        };
      }
      return null;
    } catch (error) {
      console.error('❌ Erro ao buscar usuário FDV por email:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async create(usuario: Omit<UsuarioFDV, 'CODUSUARIO' | 'DATACRIACAO' | 'DATAATUALIZACAO'>): Promise<UsuarioFDV> {
    let connection: oracledb.Connection | undefined;

    try {
      // Verificar se email já existe na empresa
      const usuarioExistente = await this.getByEmail(usuario.EMAIL, usuario.ID_EMPRESA);
      if (usuarioExistente) {
        throw new Error('Email já cadastrado para esta empresa');
      }

      // Senha sem criptografia
      const senha = usuario.SENHA || 'senha123';

      connection = await getOracleConnection();

      const result = await connection.execute(
        `
        INSERT INTO AD_USUARIOSVENDAS (
          ID_EMPRESA, NOME, EMAIL, SENHA, FUNCAO, STATUS, AVATAR, CODVEND
        ) VALUES (
          :idEmpresa, :nome, :email, :senha, :funcao, :status, :avatar, :codVend
        ) RETURNING CODUSUARIO INTO :codUsuario
        `,
        {
          idEmpresa: usuario.ID_EMPRESA,
          nome: usuario.NOME,
          email: usuario.EMAIL,
          senha: senha,
          funcao: usuario.FUNCAO || 'Vendedor',
          status: usuario.STATUS || 'ativo',
          avatar: usuario.AVATAR || null,
          codVend: usuario.CODVEND || null,
          codUsuario: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
        },
        { autoCommit: true }
      );

      const codUsuario = (result.outBinds as any).codUsuario[0];
      const novoUsuario = await this.getById(codUsuario);

      if (!novoUsuario) {
        throw new Error('Erro ao recuperar usuário criado');
      }

      return novoUsuario;
    } catch (error) {
      console.error('❌ Erro ao criar usuário FDV:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async update(codUsuario: number, usuario: Partial<UsuarioFDV>): Promise<UsuarioFDV> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      const updates: string[] = [];
      const binds: any = { codUsuario };

      if (usuario.NOME !== undefined) {
        updates.push('NOME = :nome');
        binds.nome = usuario.NOME;
      }

      if (usuario.EMAIL !== undefined) {
        updates.push('EMAIL = :email');
        binds.email = usuario.EMAIL;
      }

      if (usuario.SENHA !== undefined && usuario.SENHA.trim() !== '') {
        updates.push('SENHA = :senha');
        binds.senha = usuario.SENHA;
      }

      if (usuario.FUNCAO !== undefined) {
        updates.push('FUNCAO = :funcao');
        binds.funcao = usuario.FUNCAO;
      }

      if (usuario.STATUS !== undefined) {
        updates.push('STATUS = :status');
        binds.status = usuario.STATUS;
      }

      if (usuario.AVATAR !== undefined) {
        updates.push('AVATAR = :avatar');
        binds.avatar = usuario.AVATAR || null;
      }

      if (usuario.CODVEND !== undefined) {
        updates.push('CODVEND = :codVend');
        binds.codVend = usuario.CODVEND || null;
      }

      if (updates.length === 0) {
        throw new Error('Nenhum campo para atualizar');
      }

      const query = `
        UPDATE AD_USUARIOSVENDAS 
        SET ${updates.join(', ')}
        WHERE CODUSUARIO = :codUsuario
      `;

      await connection.execute(query, binds, { autoCommit: true });

      const usuarioAtualizado = await this.getById(codUsuario);

      if (!usuarioAtualizado) {
        throw new Error('Erro ao recuperar usuário atualizado');
      }

      return usuarioAtualizado;
    } catch (error) {
      console.error('❌ Erro ao atualizar usuário FDV:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async delete(codUsuario: number): Promise<boolean> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      await connection.execute(
        `DELETE FROM AD_USUARIOSVENDAS WHERE CODUSUARIO = :codUsuario`,
        { codUsuario },
        { autoCommit: true }
      );

      return true;
    } catch (error) {
      console.error('❌ Erro ao deletar usuário FDV:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  },

  async search(termo: string, idEmpresa?: number): Promise<UsuarioFDV[]> {
    let connection: oracledb.Connection | undefined;

    try {
      connection = await getOracleConnection();

      let query = `
        SELECT 
          u.CODUSUARIO,
          u.ID_EMPRESA,
          u.NOME,
          u.EMAIL,
          u.SENHA,
          u.FUNCAO,
          u.STATUS,
          u.AVATAR,
          u.DATACRIACAO,
          u.DATAATUALIZACAO,
          u.CODVEND,
          c.EMPRESA,
          c.CNPJ
        FROM AD_USUARIOSVENDAS u
        INNER JOIN AD_CONTRATOS c ON u.ID_EMPRESA = c.ID_EMPRESA
        WHERE (
          UPPER(u.NOME) LIKE '%' || UPPER(:termo) || '%'
          OR UPPER(u.EMAIL) LIKE '%' || UPPER(:termo) || '%'
          OR UPPER(u.FUNCAO) LIKE '%' || UPPER(:termo) || '%'
        )
      `;

      const binds: any = { termo };

      if (idEmpresa) {
        query += ` AND u.ID_EMPRESA = :idEmpresa`;
        binds.idEmpresa = idEmpresa;
      }

      query += ` ORDER BY u.NOME`;

      const result = await connection.execute(query, binds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT
      });

      const usuarios = (result.rows as any[]) || [];
      
      // Garantir que SENHA seja string e não null/undefined
      return usuarios.map(u => ({
        ...u,
        SENHA: u.SENHA ? String(u.SENHA) : ''
      }));
    } catch (error) {
      console.error('❌ Erro ao buscar usuários FDV:', error);
      throw error;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  }
};
