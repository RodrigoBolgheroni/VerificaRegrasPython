import pandas as pd
from sqlalchemy import create_engine
from config import sqlserver_config
import pandas as pd
import io
import datetime
from azure.storage.blob import BlobClient,generate_blob_sas, BlobSasPermissions

def create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual):
    # Definindo o horário de início e de expiração para o SAS Token
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(days=7)
    
    # String de conexão com o Azure Blob Storage
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"
    
    # Criando o cliente do Blob
    blob_client = BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem['ContainerName'],
     blob_name=f'{row_tabelaorigem["CaminhoOrigem"]}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx')
    
    # Gerando o SAS Token com permissões de leitura
    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=keyblob,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )
    
    # Imprimindo a URL com o SAS Token para depuração
    print(f"{blob_client.url}?{sas_token}")
    
    # Retornando a URL com SAS Token
    return f"{blob_client.url}?{sas_token}"


def UploadBlob(dic, dataframe, row_tabelaorigem, row_credenciais, keyblob, tipo):

    # Gerando a data e hora atual formatada
    dataatual = str(datetime.datetime.now())[:19].replace('-', '').replace(' ', '_').replace(':', '')
    
    # Conectando com o Azure Blob Storage usando a connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"
    
    # Convertendo o DataFrame pandas para BytesIO e escrevendo no formato Excel
    writer = io.BytesIO()
    dataframe.to_excel(writer, index=False)

    # Upload do arquivo para o Blob
    blob_name = f'{row_tabelaorigem["CaminhoOrigem"]}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx'
    BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem['ContainerName'],
                                      blob_name=blob_name).upload_blob(writer.getvalue(), blob_type="BlockBlob", connection_timeout=1000)
    
    # Atualizando o dicionário com o link do artefato
    sas_url = create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual)
    if 'Artefatos: \n' in dic:
        dic['Artefatos: \n'].append(f'{tipo}: \n{sas_url}')
    else:
        dic['Artefatos: \n'] = [f'{tipo}: \n{sas_url}']
    
    return dic


def VariaveisPipeline(): 
    # Cria variáveis úteis durante o pipeline
    dic = {}

    # Definindo os dados e as colunas do DataFrame analítico
    dados_analitico = {
        "Coluna": [],
        "Linha": [],
        "Valor": [],
        "Critica": []
    }

    # Definindo os dados e as colunas do DataFrame agrupado
    dados_agrupado = {
        "Coluna": [],
        "Critica": [],
        "Valor": [],
        "Linhas com problemas (exemplo)": []
    }

    # Criando os DataFrames usando pandas
    analiticofinal = pd.DataFrame(dados_analitico)
    agrupadofinal = pd.DataFrame(dados_agrupado)

    return dic, analiticofinal, agrupadofinal



def create_delta_table(df, row_tabelaorigem, key, tabela_validacoes, cliente, row_carga, linkedservice):
    # Supondo que você tenha uma tabela delta em pandas, sem o Spark.
    
    # Remover as colunas desnecessárias de validação
    valid_columns = tabela_validacoes[~tabela_validacoes['DescColuna'].isin(['ChaveMD5', 'Encoding', 'Header'])]
    
    # Mapeamento de colunas para as novas descrições
    new_columns = valid_columns[['Coluna', 'DescColuna']].set_index('Coluna').to_dict()['DescColuna']
    
    # Renomear as colunas no DataFrame df de acordo com o mapeamento
    df = df.rename(columns=new_columns)
    
    # Adicionando a coluna "DataInsercao"
    df['DataInsercao'] = pd.to_datetime('now') - pd.Timedelta(hours=3)
    
    # Adicionando a coluna "ArquivoFonte"
    df['ArquivoFonte'] = row_tabelaorigem['TabelaOrigem']
    
    # Substituindo valores vazios por NaN
    df = df.replace("", pd.NA)
    
    # Aqui você deve decidir como salvar o DataFrame, pois não há suporte para o formato delta sem Spark.
    # Uma solução alternativa pode ser salvar em formato CSV, Parquet ou outro.
    output_path = f"{row_tabelaorigem['ContainerName']}/{row_tabelaorigem['CaminhoOrigem']}/deltatable.csv"
    df.to_csv(output_path, index=False)

    return df



