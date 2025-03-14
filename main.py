import pandas as pd
import traceback
from datetime import datetime, timedelta
from utils.db_functions import SqlServerQuery, ExecuteScriptMySQL,Credential,MysqlQuery  # Funções para consultar o SQL Server e MySQL
from utils.file_utils import DetectFormatoEncoding, ValidacaoHeader, RenameColunas, IsChaveUnica

def Main(cliente,tipoarquivo,server_mysql, user_mysql, password_mysql, bancodedados_mysql):
    query_tblarquivo_import = f"""SELECT * FROM tblpbiarquivoimport WHERE Status = 'NaoIniciado' OR Status = 'NaFila' AND TipoArquivo = '{tipoarquivo}' 
        """ 
    tabela_arquivoimport = MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql,query_tblarquivo_import)
    for _,row_tabelaarquivo_import in tabela_arquivoimport.iterrows():  
        try:
            # 1. Consultar as tabelas de configuração no SQL Server
            # Consulta a tabela de arquivo com base no cliente e no tipo arquivo
            query_cliente_tipoarquivo = f"""SELECT cta.[IdCliente_TipoArquivo],
                                            cta.[IdCliente],
                                            cta.[IdTipoArquivo],
                                            cta.[IdExtensaoArquivo],
                                            cta.[Encoding],
                                            cta.[IsHeader],
                                            cta.[Header],
                                            cta.[Chave],
                                             c.Cliente AS ClienteNome, 
                                            ta.TipoArquivo AS TipoArquivoNome, 
                                            ea.ExtensaoArquivo AS ExtensaoNome
                                        FROM [monitor].[tblcliente_tipoarquivo] cta 
                                        INNER JOIN [monitor].[tblcliente] c ON cta.IdCliente = c.IdCliente
                                        INNER JOIN [monitor].[tbltipoarquivo] ta ON cta.IdTipoArquivo = ta.IdTipoArquivo
                                        INNER JOIN [monitor].[tblextensaoarquivo] ea ON cta.IdExtensaoArquivo = ea.IdExtensaoArquivo WHERE c.Cliente = '{cliente}' AND ta.TipoArquivo = '{tipoarquivo}' AND cta.Ativo = 1 
            """ 
            tblcliente_tipoarquivo = SqlServerQuery(query_cliente_tipoarquivo)
            for _,row_tabelacliente_tipoarquivo in tblcliente_tipoarquivo.iterrows():continue

            #consulta a tabela de regras com base no IdCliente_TipoArquivo da tabela de arquivos
            query_regras = f"""SELECT ctr.[IdCliente_Tipoarquivo_Regra] AS Id,
	                            ctr.[DescricaoCampo],
	                            r.Regra AS RegraNome,
	                            ctr.[TipoDeDado],
	                            ctr.[Obrigatorio],
	                            ctr.[IdCliente_TipoArquivo],
                                ctr.[IdRegra]
                            FROM [monitor].[tblcliente_tipoarquivo_regra] ctr
                            INNER JOIN [monitor].[tblregra] r ON ctr.IdRegra = r.IdRegra 
                            WHERE IdCliente_TipoArquivo = {row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo']} AND r.Ativo = 1
            """
            tblcliente_tipoarquivo_regra = SqlServerQuery(query_regras)

            # 2. Carregar o arquivo CSV local
            caminho_csv = 'BASE_FAT_SAP_20250306_225959_001.csv'
            try:
                # Verifica se o arquivo tem cabeçalho
                if tblcliente_tipoarquivo['IsHeader'].iloc[0] == 1:
                    df = pd.read_csv(caminho_csv, encoding='Windows-1252', sep=';')
                    print("Arquivo CSV carregado com sucesso (com cabeçalho)!")
                else:
                    df = pd.read_csv(caminho_csv, encoding='Windows-1252', header=None, sep=';')
                    print("Arquivo CSV carregado com sucesso (sem cabeçalho)!")
                print("DataFrame inicial (após leitura do arquivo):")
                print(df.head())  # Exibe as primeiras linhas do DataFrame
            except Exception as e:
                print(f"Erro ao carregar o arquivo CSV: {e}")
                print(traceback.format_exc())
                return

            # 3. Validar formato e encoding do arquivo
            dic = {}  # Dicionário para armazenar resultados

            # Aplicar as validações de formato e encoding
            dic, flag_encode_correto, encode_recebido = DetectFormatoEncoding(
                dic, row_tabelacliente_tipoarquivo, caminho_csv
            )
            if not flag_encode_correto:
                print("Erro na validação do formato ou encoding do arquivo.")
                print(dic)  # Exibe o relatório de erros
                return

            # Processar o arquivo (com ou sem cabeçalho)
            try:
                    # Se o arquivo não tiver cabeçalho, atribuir nomes de colunas manualmente
                    if row_tabelacliente_tipoarquivo['IsHeader'] == 0:
                        nomes_colunas_esperadas = row_tabelacliente_tipoarquivo['Header'].split(',')  # Nomes das colunas esperadas
                        df.columns = nomes_colunas_esperadas  # Atribui os nomes das colunas manualmente
                        print("Nomes das colunas atribuídos manualmente (arquivo sem cabeçalho).")

                    # Renomeia as colunas com base no campo 'Header'
                    df, dic = RenameColunas(df, row_tabelacliente_tipoarquivo, dic)

                    # Valida o cabeçalho do arquivo após renomear as colunas
                    nomes_colunas_esperadas = row_tabelacliente_tipoarquivo['Header'].split(',')  # Nomes das colunas esperadas
                    dic = ValidacaoHeader(df.columns, dic, nomes_colunas_esperadas)

                    # Valida a chave MD5
                    colunas_chave = row_tabelacliente_tipoarquivo['Chave'].split(',')  # Colunas que compõem a chave MD5
                    colunas_faltantes = [coluna for coluna in colunas_chave if coluna not in df.columns]

                    if colunas_faltantes:
                        dic['Erro_Chave_MD5'] = f"Colunas faltantes para a chave MD5: {', '.join(colunas_faltantes)}"
                        print(dic)
                    else:
                        dic, dfchave_repetidas = IsChaveUnica(df, colunas_chave, dic)
                        print('Chave MD5 validada\n')

            except Exception as e:
                print(f"Erro ao validar o arquivo: {e}")
                print(traceback.format_exc())
                return

            print("Formato, encoding e cabeçalho do arquivo validados com sucesso!")

            # Exibe o DataFrame final após todas as validações e transformações
            print("\nDataFrame final (após todas as validações e transformações):")
            print(df)

        except Exception as e:
            print(f"Erro durante a execução: {e}")
            print(traceback.format_exc())
    print('Não á arquivos')
# Executar a função Main
Main(cliente,tipoarquivo,server_mysql, user_mysql, password_mysql, bancodedados_mysql)
