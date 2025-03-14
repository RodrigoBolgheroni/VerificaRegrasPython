import pandas as pd
import sys
sys.path.append('c:/Users/thmalta/Desktop/Faturamento')
from config.mysql_config import conecta_mysql
from config.sqlserver_config import conecta_sqlserver
from sqlalchemy import create_engine
import urllib.parse
import traceback
import datetime
from datetime import timedelta

def Credential(nome_cliente):
    # Estabelecer a conexão com o banco de dados
    conn = conecta_sqlserver()
    
    # Consulta SQL
    query = f"""SELECT * FROM [monitor].[tblcredenciaisfaturamento] WHERE ativo = 1 AND cliente = '{nome_cliente}'"""
    
    # Carregar os dados em um DataFrame
    tabela_credenciais = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_credenciais




def MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql,query):
    # Estabelecendo a conexão com o MySQL usando mysql.connector
    conn = conecta_mysql(server_mysql, user_mysql, password_mysql, bancodedados_mysql)
    
    # Carregar os dados da consulta SQL em um DataFrame
    tabela_arquivoimport = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_arquivoimport




def SqlServerQuery(query):
    # Estabelecendo a conexão com o SQL 
    conn = conecta_sqlserver()
    
    # Carregar os dados da consulta SQL em um DataFrame
    tabela_validacoes = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_validacoes


def ExecuteScriptMySQL(row_credenciais, query): 
    # Codificando a senha, caso contenha caracteres especiais
    password_mysql_encoded = urllib.parse.quote_plus(row_credenciais.MySQLSenha)
    
    # String de conexão usando sqlalchemy
    connection_string = f"mysql+mysqlconnector://{row_credenciais.MySQLUsuario}:{password_mysql_encoded}@{row_credenciais.MySQLServidor}/{row_credenciais.MySQLBancoDeDados}"
    engine = create_engine(connection_string)
    
    # Usando pandas para executar a query
    with engine.connect() as connection:
        connection.execute(query)


def ExecuteScriptSqlServer(row_carga, script):
    # Codificando a senha, caso contenha caracteres especiais
    password_encoded = urllib.parse.quote_plus(row_carga.Password)
    
    # String de conexão usando sqlalchemy para SQL Server
    connection_string = f"mssql+pyodbc://{row_carga.User}:{password_encoded}@{row_carga.Server}/{row_carga.BancoDeDados}?driver=ODBC+Driver+18+for+SQL+Server"
    
    # Criando o engine com a string de conexão
    engine = create_engine(connection_string)
    
    # Usando engine para executar o script
    try:
        with engine.connect() as connection:
            connection.execute(script.replace('\n', ' ').replace('\r', ' '))
        return False
    except Exception as e:
        # Captura e exibe a mensagem de erro
        print(f"Erro SQL: {str(e)}")
        return True




def CargaSqlServer(cliente, row_tabelaorigem, row_carga, linkedservice, tabela_validacoes):
    # Aqui vamos assumir que o SAS token é passado diretamente em linkedservice
    blob_sas_token = linkedservice['sas_token']  # Modifique aqui conforme necessário, se o SAS token vem de outro lugar
    
    # Caminho para o arquivo Delta
    path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/deltatable'
    
    # Lê o arquivo Delta (convertido para CSV ou Parquet com pandas)
    df_delta = pd.read_parquet(path)  # Substitua por pd.read_csv(path) se o arquivo for CSV
    
    # Obtendo a lista de colunas obrigatórias a partir da tabela de validações
    row_colunasobt = tabela_validacoes[tabela_validacoes['Validacao'] == 'ColunasObrigatoriasTabela'].iloc[0]
    colunas_obrigatorias = row_colunasobt['Valores'].split(',')
    
    # Valida se as colunas obrigatórias estão presentes no DataFrame
    for coluna in colunas_obrigatorias:
        if coluna not in df_delta.columns:
            raise ValueError(f"A coluna obrigatória '{coluna}' não está presente no arquivo.")
    
    # Converte a coluna DataInsercao para o formato datetime
    df_delta['DataInsercao'] = pd.to_datetime(df_delta['DataInsercao'], errors='coerce')  # Trata valores inválidos
    
    # String de conexão usando SQLAlchemy para SQL Server
    password_encoded = urllib.parse.quote_plus(row_carga.Password)
    connection_string = f"mssql+pyodbc://{row_carga.User}:{password_encoded}@{row_carga.Server}/{row_carga.BancoDeDados}?driver=ODBC+Driver+18+for+SQL+Server"
    engine = create_engine(connection_string)
    
    # Adicionando as colunas de controle
    df_delta['ArquivoFonte'] = row_tabelaorigem.TabelaOrigem
    df_delta['DataInsercao'] = pd.to_datetime('now') - pd.Timedelta(hours=3)  # Ajusta a hora conforme necessário
    
    # Envia os dados para o SQL Server
    try:
        df_delta.to_sql(
            f"{row_carga.TabelaDestinoCarga}diaria", 
            con=engine,
            if_exists='append', 
            index=False,
            method='multi'
        )
        print("Carga concluída com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar dados no SQL Server: {e}")



def insert_temp_table(df, row_tabelaorigem, row_credenciais, cliente):
    try:
        # Criar a tabela temporária (caso não exista)
        create_temp_table_query = """
        IF OBJECT_ID('tempdb..#tblTemp') IS NULL
        CREATE TABLE #tblTemp (
            coluna1 VARCHAR(255),
            coluna2 INT,
            coluna3 VARCHAR(255)
        );
        """
        ExecuteScriptSqlServer(row_credenciais, create_temp_table_query)

        # Inserir dados em blocos para eficiência
        values = []
        for index, row in df.iterrows():
            values.append(f"('{row['coluna1']}', {row['coluna2']}, '{row['coluna3']}')")
        
        # Inserir em uma única query com múltiplos valores
        if values:
            insert_query = f"""
            INSERT INTO #tblTemp (coluna1, coluna2, coluna3)
            VALUES {', '.join(values)}
            """
            ExecuteScriptSqlServer(row_credenciais, insert_query)
        
        print('Dados inseridos na tabela temp')

        # Mover os dados para a tabela final
        transfer_query = """
        INSERT INTO tabela_final (coluna1, coluna2, coluna3)
        SELECT coluna1, coluna2, coluna3
        FROM #tblTemp;
        """
        ExecuteScriptSqlServer(row_credenciais, transfer_query)
        print('Dados movidos para a tabela final')

        # Limpar a tabela temporária
        clean_up_query = "DROP TABLE IF EXISTS #tblTemp;"
        ExecuteScriptSqlServer(row_credenciais, clean_up_query)
        print('Tabela temp removida')

    except Exception as e:
        # Tratar erro e atualizar status no banco de dados
        print("Erro ao inserir dados na tabela temporária:")
        print(e)
        print("Stack Trace:")
        print(traceback.format_exc())

        # Atualizar status de erro no banco de dados
        data = datetime.datetime.now() - timedelta(hours=3)
        datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
        query_atterro = f"""
        UPDATE tblpbiarquivoimport 
        SET status='Erro',
            pipelinename = 'Faturamento',
            dataultimaexecucao= '{datahorastr}',
            pipelinetriggername='Faturamento{cliente}',
            workspacename='prodsynapse01' 
        WHERE id = {row_tabelaorigem.Id}
        """
        ExecuteScriptMySQL(row_credenciais, query_atterro)
        print('Erro ao processar a carga de dados')











