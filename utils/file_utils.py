import chardet 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import reduce
from datetime import timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, BlobSasPermissions, generate_blob_sas
import pandas as pd  
from functools import reduce


def DetectFormatoEncoding(dic, row_tabelacliente_tipoarquivo, caminho_csv):
    """
    Valida o formato e o encoding do arquivo CSV.
    
    :param dic: Dicionário para armazenar resultados.
    :param row_tabelacliente_tipoarquivo: Linha da tabela tblcliente_tipoarquivo.
    :param row_tabelavalidacao: Linha da tabela tblcliente_tipoarquivo_regra.
    :param caminho_csv: Caminho do arquivo CSV local.
    :return: Tuple (dic, flag_encode_correto, encode_recebido).
    """
    # Validação do formato do arquivo
    if row_tabelacliente_tipoarquivo['ExtensaoNome'] == caminho_csv.split("."):
        # Adiciona informações ao relatório
        dic['Reporte'] = [
            f'<br>Neste Report Será Feita Análise De Integridade Do Cliente {row_tabelacliente_tipoarquivo["ClienteNome"]} '
            f'Para O Arquivo Do Tipo {row_tabelacliente_tipoarquivo["TipoArquivoNome"]}', ''
        ]

        # Validação do formato (deve ser .csv)
    else:
        dic['Encoding'] = [
            f'<br>O Formato Do Arquivo Está Incorreto E Deve Ser .csv. '
            f'O Arquivo Enviado É {row_tabelacliente_tipoarquivo["ExtensaoNome"][-4:]}.<br>', ''
        ]
        return dic, False, None  # Formato incorreto, interrompe a validação

    # Validação do encoding
    try:
            with open(caminho_csv, 'rb') as f:
                blob_data = f.read()  # Lê o conteúdo do arquivo em modo binário

            # Detecta o encoding do arquivo
            encode_arquivo = chardet.detect(blob_data)['encoding']


            # Verifica se o encoding detectado é o esperado
            if encode_arquivo != row_tabelacliente_tipoarquivo["Encoding"]:
                dic['Encoding'] = [
                    f'<br>O Encoding Do Arquivo Está Incorreto E Deve Ser {row_tabelacliente_tipoarquivo["Encoding"]}, '
                    f'Mas O Arquivo Enviado Tem O Encoding {encode_arquivo}.<br>', ''
                ]
                print('erro')
                return dic, False, encode_arquivo  # Encoding incorreto
            else:
                print('Verificado')
                return dic, True, encode_arquivo  # Encoding correto

    except Exception as e:
            print(f"Erro ao validar o encoding do arquivo: {e}")
            return dic, False, None
            
def ValidacaoHeader(dfcolumns, dic, listanomecolunas):
    """
    Verifica se todas as colunas esperadas estão presentes no DataFrame.

    Args:
        dfcolumns (list): Lista de colunas do DataFrame.
        dic (dict): Dicionário para armazenar resultados.
        listanomecolunas (list): Lista de nomes de colunas esperadas.

    Returns:
        dic (dict): Dicionário atualizado com resultados da validação.
    """
    try:
        # Verifica quais colunas não estão no DataFrame
        colunas_faltantes = [coluna for coluna in listanomecolunas if coluna not in dfcolumns]

        # Se houver colunas faltantes, adiciona uma descrição no dicionário
        if colunas_faltantes:
            colunas_erradas = '\n'.join(colunas_faltantes)
            dic['Descrição Colunas'] = [
                f'Existem colunas do layout que não foram enviadas no arquivo. A relação segue abaixo:\n\n{colunas_erradas}.'
            ]

        return dic

    except Exception as e:
        print(f'Erro ao validar o cabeçalho: {e}')
        return dic


def RenameColunas(df, row_tabelaorigem, dic):
    """
    Renomeia as colunas do DataFrame com base nos nomes definidos no campo 'Header' da tblcliente_tipoarquivo.

    Args:
        df (pd.DataFrame): DataFrame a ser processado.
        row_tabelaorigem (pd.Series): Linha da tblcliente_tipoarquivo com as configurações do cliente e tipo de arquivo.
        dic (dict): Dicionário para armazenar resultados.

    Returns:
        df (pd.DataFrame): DataFrame com as colunas renomeadas.
        dic (dict): Dicionário atualizado com resultados da validação.
    """
    try:
        # Obtém os nomes das colunas do campo 'Header'
        nomes_colunas = row_tabelaorigem['Header'].split(',')  # Supondo que os nomes estejam separados por vírgula

        # Verifica se o número de colunas do arquivo corresponde ao número de colunas esperado
        if len(nomes_colunas) != df.shape[1]:
            dic['Erro_Numero_Colunas'] = f"Erro: O arquivo possui {df.shape[1]} colunas, mas era esperado {len(nomes_colunas)}."
            return df, dic

        # Renomeia as colunas do DataFrame
        df.columns = nomes_colunas

        # Atualiza o dicionário com o resultado da validação
        dic['Colunas_Renomeadas'] = "Colunas renomeadas com sucesso."
        print("Colunas renomeadas com base no campo 'Header' da tblcliente_tipoarquivo.")

        return df, dic

    except Exception as e:
        dic['Erro_RenameColunas'] = f"Erro ao renomear colunas: {e}"
        print(f"Erro ao renomear colunas: {e}")
        return df, dic


def LeArquivo(dic, row_tabelaorigem, encode, cliente, caminho_arquivo):
    """
    Lê um arquivo Excel ou CSV local, processa as colunas e retorna um DataFrame e um dicionário.

    :param dic: Dicionário para armazenar resultados.
    :param row_tabelaorigem: Linha da tabela de origem com informações do arquivo.
    :param encode: Encoding do arquivo.
    :param cliente: Nome do cliente.
    :param caminho_arquivo: Caminho local do arquivo.
    :return: DataFrame processado e dicionário.
    """
    try:
        # Lê o arquivo com base na extensão já validada
        if row_tabelaorigem['TipoArquivoNome'][-5:] == '.xlsx':
            df = pd.read_excel(caminho_arquivo, dtype='str')
            print("Arquivo Excel lido com sucesso!")
        else:
            df = pd.read_csv(caminho_arquivo, encoding=encode, dtype='str',sep=";")
            print("Arquivo CSV lido com sucesso!")
        # Renomeia as colunas
        df, dic = RenameColunas(df, row_tabelaorigem, dic)

        # Retorna o DataFrame final e o dicionário
        return df, dic

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return None, dic
def IsChaveUnica(df, colunas_chave, dic):
    """
    Valida se as colunas que compõem a chave MD5 são únicas no DataFrame.

    Args:
        df (pd.DataFrame): DataFrame a ser validado.
        colunas_chave (list): Lista de colunas que compõem a chave MD5.
        dic (dict): Dicionário para armazenar resultados.

    Returns:
        dic (dict): Dicionário atualizado com resultados da validação.
        dfchave_repetidas (pd.DataFrame): DataFrame com as linhas que têm chaves duplicadas.
    """
    try:
        # Verifica se as colunas da chave MD5 estão presentes no DataFrame
        colunas_faltantes = [coluna for coluna in colunas_chave if coluna not in df.columns]

        if colunas_faltantes:
            # Se houver colunas faltantes, atualiza o dicionário com a mensagem de erro
            dic['Erro_Validacao_Chave'] = f"Colunas faltantes para a chave MD5: {', '.join(colunas_faltantes)}"
            return dic, None

        # Verifica se há valores duplicados nas colunas da chave MD5
        duplicados = df[df.duplicated(subset=colunas_chave, keep=False)]

        if not duplicados.empty:
            # Se houver duplicados, atualiza o dicionário com a mensagem de erro
            dic['Erro_Validacao_Chave'] = f"Chave MD5 não é única. Foram encontradas {len(duplicados)} linhas duplicadas."
            return dic, duplicados

        # Se a chave MD5 for válida, atualiza o dicionário com sucesso
        dic['Sucesso_Validacao_Chave'] = "Chave MD5 validada com sucesso."
        return dic, None

    except Exception as e:
        # Em caso de erro, atualiza o dicionário com a mensagem de erro
        dic['Erro_Validacao_Chave'] = f"Erro ao validar chave única: {str(e)}"
        return dic, None

