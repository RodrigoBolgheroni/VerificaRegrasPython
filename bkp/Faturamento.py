#!/usr/bin/env python
# coding: utf-8

# ## Faturamento
# 
# 
# 

# In[30]:


get_ipython().system('pip install chardet')
get_ipython().system('pip install mysql-connector-python')


# In[112]:


from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, when,monotonically_increasing_id, lit,collect_list, concat_ws, unix_timestamp, date_format, to_timestamp, regexp_replace
import pyspark.pandas as ps
from pyspark.sql import Window, SparkSession
from azure.storage.blob import BlobServiceClient, BlobClient
import chardet 
from pyspark.sql.functions import udf, countDistinct, count,  row_number, collect_set, rank, to_date
from pyspark.sql.types import StringType
import re
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import reduce
import datetime
from datetime import timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, BlobSasPermissions, generate_blob_sas
import io
import traceback
from pyspark.sql import functions as F
import pyodbc 
import mysql.connector
import pandas as pd


def DetectFormatoEncoding(dic, row_tabelaorigem, row_tabelavalidacao, key):
    # Primeira chave é o código que sai do chardet.detect, a chave do segundo é o valor que o pyspark vai ler. 
    # O terceiro o valor q vai ser printado no email e comparado com tabela do sql server
    if row_tabelaorigem.TabelaOrigem[-5:] in ['.xlsx']:
        return dic, True, '.xslx'
    else:
        chardet_to_read = {'ISO-8859-1': {'windows-1252':'ANSI'},
                            'UTF-8-SIG':{'utf-8': 'utf-8'},
                            'utf-8':{'utf-8': 'utf-8'}}
        dic['Reporte'] = [f'<br>Neste report será feita análise de integridade do cliente {row_tabelavalidacao.Cliente} para o arquivo do tipo {row_tabelaorigem.TipoArquivo}, nome original {row_tabelaorigem.NomeOriginal} e nome no blob {row_tabelaorigem.TabelaOrigem}.<br>', '']
        if row_tabelaorigem.TabelaOrigem[-4:] not in ['.CSV', '.csv']:
            dic['Encoding'] = [f'<br>O formato do arquivo está incorreto e deve ser .csv quando o arquivo enviado é {row_tabelaorigem.TabelaOrigem[-4:]}.<br>', '']
            #mandar email e parar loop
        else:
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem.ContainerName};AccountKey={key};EndpointSuffix=core.windows.net"

            blob_client = BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem.ContainerName, blob_name=f'{row_tabelaorigem.CaminhoOrigem}/{row_tabelaorigem.TabelaOrigem}')
            blob_data = blob_client.download_blob().readall()

            encode_arquivo = chardet.detect(blob_data)['encoding']
            # mudar para diferente
            if list(chardet_to_read[encode_arquivo].keys())[0] != row_tabelavalidacao.Valores:
                encode = chardet_to_read[encode_arquivo][list(chardet_to_read[encode_arquivo].keys())[0]]
                dic['Encoding'] = [f'<br>O encoding do arquivo está incorreto e deve ser {row_tabelavalidacao.Valores}, mas o arquivo enviado tem o encoding {encode}.<br>', '']
                #del blob_data
                return dic, False, list(chardet_to_read[encode_arquivo].keys())[0]
            else:
                return dic, True, list(chardet_to_read[encode_arquivo].keys())[0]

def ValidacaoHeader(dfcolumns, dic, listanomecolunas):
    lista_nomes = [i+'\n' for i in listanomecolunas if i not in dfcolumns]
    colunas_erradas = ''.join(lista_nomes)
    if len(lista_nomes)>0:
        dic['Descrição Colunas'] = [f'Existem colunas do layout que não foram enviadas no arquivo. A relação segue abaixo:\n\n {colunas_erradas}.']
    return dic

def RenameColunas(df_toschema, tabela_validacoes, dic):
    old_columns = pd.DataFrame(df_toschema.schema.names, columns=['DescColuna'])
    df_colunaspadrao = tabela_validacoes.filter(col('Coluna').startswith('_c')).select(['Coluna', 'DescColuna']).toPandas()
    df_aux = old_columns.merge(df_colunaspadrao, how='left', on='DescColuna')
    columns_to_drop_index = [i+1 for i in df_aux[df_aux['Coluna'].isna()].index.tolist()]
    columns_to_drop_name = df_aux[df_aux['Coluna'].isna()]['DescColuna'].tolist()
    if 'Linha' in columns_to_drop_name:
        columns_to_drop_name.remove('Linha')
    df_toschema = df_toschema.drop(*columns_to_drop_name)
    oldColumns = df_aux[~df_aux['Coluna'].isna()]['DescColuna'].tolist()
    newColumns = df_aux[~df_aux['Coluna'].isna()]['Coluna'].tolist()
    df_toschema = reduce(lambda df_toschema, idx: df_toschema.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df_toschema)
    coluns_to_validate = tabela_validacoes.filter(tabela_validacoes.Coluna.like('_c%')).select('DescColuna').toPandas()['DescColuna'].tolist()
    try:
        dic = ValidacaoHeader(oldColumns, dic, coluns_to_validate)
        print('Nomes colunas validadas\n')
        return dic, df_toschema, columns_to_drop_index
    except:
        print('Erro validação nome colunas')


def LeArquivo(dic, row_tabelaorigem, row_tabelavalidacao, tabela_validacoes, encode, cliente, key, linkedservice):

    if row_tabelaorigem.TabelaOrigem[-5:] in ['.xlsx']:
        path = f"abfss://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.dfs.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/{row_tabelaorigem.TabelaOrigem}"
        df_excel = pd.read_excel(path,
                            storage_options = {'account_key' : key},
                            dtype='str')
        df_excel['Linha'] = range(1, df_excel.shape[0] + 1)
        df = spark.createDataFrame(df_excel)
        dic, df_toschema, columns_to_drop_index = RenameColunas(df, tabela_validacoes, dic)
        # print(df_toschema.columns)
        # print(columns_to_drop_index)
        # print([df_toschema.columns[i] for i in columns_to_drop_index])
        # df_toschema = df_toschema.drop(*[df_toschema.columns[i] for i in columns_to_drop_index])
        df_toschema.createOrReplaceTempView("viewdf")
        return df_toschema, dic

    if row_tabelaorigem.TabelaOrigem[-4:] in ['.CSV', '.csv']:
        sc = SparkSession.builder\
                            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.4.5.3.20241016.1')\
                            .getOrCreate()
        token_library = sc._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
        blob_sas_token = token_library.getConnectionString(linkedservice)

        spark.conf.set(
                'fs.azure.sas.%s.%s.blob.core.windows.net' % (row_tabelaorigem.ContainerName, row_tabelaorigem.ContainerName),
                blob_sas_token)

        path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/{row_tabelaorigem.TabelaOrigem}'
        if row_tabelavalidacao.Valores == 'True':
            df_toschema = spark.read.load(path, format='csv', header=True, sep=';', encoding='utf-8')
            dic, df_toschema, columns_to_drop_index = RenameColunas(df_toschema, tabela_validacoes, dic)
        if row_tabelavalidacao.Valores=='False':
            df_toschema = spark.read.load(path, format='csv', header=False, sep=';', encoding=encode)

        df = dfZipWithIndex(df_toschema, path, spark, row_tabelavalidacao, encode, key, row_tabelaorigem, columns_to_drop_index, colName="Linha")
        df.createOrReplaceTempView("viewdf")
        return df, dic

def dfZipWithIndex(df, path, spark, row_tabelavalidacao, encode, key, row_tabelaorigem, columns_to_drop_index,colName="Linha"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe 
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
        :param sc: spark context
    '''

    new_schema = StructType([StructField('Linha',LongType(),True)]+ df.schema.fields)
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    conf.set(f'fs.azure.account.key.{row_tabelaorigem.ContainerName}.blob.core.windows.net',
                    key)
    if row_tabelavalidacao.Valores=='True':
        new_rdd = spark.sparkContext.textFile(path, use_unicode=False).zipWithIndex()\
                                                        .filter(lambda args: args[1] > 0)\
                                                        .map(lambda args:  [args[1]+1]+args[0].decode(encode).split(';'))
        new_rdd = new_rdd.map(lambda linha: list(v for i, v in enumerate(linha) if i not in columns_to_drop_index))
        return spark.createDataFrame(new_rdd, new_schema)
    else: 
        new_rdd = spark.sparkContext.textFile(path, use_unicode=False).zipWithIndex()\
                                                        .map(lambda args:  [args[1]+1]+args[0].decode(encode).split(';'))
        new_rdd = new_rdd.map(lambda linha: list(v for i, v in enumerate(linha) if i not in columns_to_drop_index))
        return spark.createDataFrame(new_rdd, new_schema)

def IsChaveUnica(dataframe, row, dic, tabela_validacoes):
    #adicionar em que linha está, concatenando com virgula
    colunas_naoenviadas = [i for i in row.Valores.split(', ') if i not in dataframe.columns]
    # colunas_toprint = ', '.join(colunas_naoenviadas)
    df_aux = tabela_validacoes.filter(tabela_validacoes['Coluna'].isin(row.Valores.split(', '))).select('DescColuna', 'Coluna').toPandas()
    lista_nomes_colunas = pd.DataFrame(colunas_naoenviadas, columns=['Coluna']).merge(df_aux, how='left')['DescColuna'].tolist()
    colunas_toprint = ', '.join(lista_nomes_colunas)
    if len(colunas_naoenviadas) > 0:
        dic['Chave Atualização']  = [f'Problema ao tentar estabelecer chave de atualização para tabela. As colunas a seguir não foram enviadas: {colunas_toprint}.']
        print(dic)
        return dic, None

    df_chave_repetidas = dataframe.groupBy(row.Valores.split(', ')).count().filter(col('count')>1)
    newColumns = tabela_validacoes.filter(tabela_validacoes['Coluna'].isin(row.Valores.split(', '))).select('DescColuna').toPandas()['DescColuna'].tolist() + ['Quantidade']
    oldColumns = df_chave_repetidas.schema.names
    df_chave_repetidas = reduce(lambda df_chave_repetidas, idx: df_chave_repetidas.withColumnRenamed(oldColumns[idx],
                                                                                                     newColumns[idx]),
                                                           range(len(oldColumns)),
                                                           df_chave_repetidas)
    nrows = df_chave_repetidas.count()
    problema = f'Ao tentar estabelecer uma chave unica com as colunas {row.DescColuna} existem registros que possuem linhas duplicadas'
    if nrows ==0:
        return dic, None
    else:
        return OutputString(df_chave_repetidas, nrows,  dic, row.Coluna, problema), df_chave_repetidas

def OutputsDataframes(dataframe, coluna):
        
    analitico_dataframe = dataframe.withColumnRenamed(coluna, "Valor")\
                                    .withColumn("Coluna", lit(coluna))\
                                    .withColumn("Linha", col("Linha").cast("string"))\
                                    .select(['Coluna', 'Linha', 'Valor', 'Critica'])
    
    agrupado_dataframe = analitico_dataframe.select(col('*'), row_number().over(Window.orderBy(['Linha'])).alias('row_number'))\
                                   .groupBy(["Critica", 'Valor']).agg(collect_list("Linha").alias("Linhas"))\
                                   .withColumn("Linhas com problemas (exemplo)", concat_ws(", ", "Linhas"))\
                                   .withColumn("Coluna", lit(coluna)) \
                                   .select(['Coluna','Critica', 'Valor', 'Linhas com problemas (exemplo)'])

    return analitico_dataframe, agrupado_dataframe

def OutputString(agrupado_dataframe, nrows, dic, coluna, problema):

    if nrows > 0:
        def limitar_texto(match):
            texto_limitado = match.group(2)[:50] 
            texto_limitado += "..."  
            return match.group(1) + texto_limitado + match.group(3)
        
        pattern = re.compile(r'(<td>)(.{51,}?)(</td>)')

        if coluna not in dic:
            dic[coluna] =   [f"""<br>Critica: {problema} <br> Número de linhas: {nrows}. <br>Abaixo segue tabela resumida agrupadas por tipo de Crítica.<br>""",
                            re.sub(pattern, limitar_texto, agrupado_dataframe.limit(5).toPandas().to_html())]
            return dic
        else:
            dic[coluna] = dic[coluna] + \
                            [f"""<br>Critica: {problema} <br> Número de linhas: {nrows}. <br>Abaixo segue tabela resumida agrupadas por tipo de Crítica.<br>""",
                            re.sub(pattern, limitar_texto, agrupado_dataframe.limit(5).toPandas().to_html())]
    return dic

def IsNull(dataframe, row, dic):
    # esta diferente em filter critica pois se n fica vazio
    dataframe_tratado = spark.sql(f"""select Linha, 
                                        {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                                        case when (({row.Coluna} is null) or ({row.Coluna} ='')) then 'Valor nulo'
                                        else 'Valor preenchido' end as Critica
                                        from viewdf where (({row.Coluna} is null) or ({row.Coluna} =''))""") 
    nrows = dataframe_tratado.count()
    problema = 'Valores que deveriam estar preenchidos, estão sendo enviados como nulo.'
    analitico_dataframe, agrupado_dataframe = OutputsDataframes(dataframe_tratado, str(row.DescColuna).replace(' ', ''))

    return analitico_dataframe, agrupado_dataframe, OutputString(agrupado_dataframe, nrows,  dic, row.DescColuna, problema)

def IsCNPJCliente(dataframe, row, dic):
    if row.ColunaObrigatoria==1:
        analitico_null_dataframe, agrupado_null_dataframe, dic = IsNull(dataframe, row, dic)
    
    dataframe_tratado = spark.sql(f"""select Linha, 
                                        {row.Coluna} as `{row.DescColuna}`,
                                        case when char_length(REPLACE(REPLACE(REPLACE({row.Coluna},".",""),"/",""),"-","")) = 14 and _c29='PJ' then 'Campo CNPJ 14 Dígitos.'
                                             when char_length(REPLACE(REPLACE(REPLACE({row.Coluna},".",""),"/",""),"-","")) = 11 and _c29='PF' then 'Campo CPF 11 Dígitos.'
                                        else 'Valor com tamanho inadequado para ser CPF ou CNPJ' end as Critica 
                                        from viewdf
                                        where (({row.Coluna} is not null) and ({row.Coluna} <>'')) and 
                                        not((char_length(REPLACE(REPLACE(REPLACE({row.Coluna},".",""),"/",""),"-","")) = 14 and _c29='PJ') or 
                                        (char_length(REPLACE(REPLACE(REPLACE({row.Coluna},".",""),"/",""),"-","")) = 11 and _c29='PF'))""")

    problema = 'Valores que deveriam ser de CNPJ ou CPF, não tem 14 ou 11 dígitos.'
    nrows = dataframe_tratado.count()
    analitico_dataframe, agrupado_dataframe = OutputsDataframes(dataframe_tratado, row.DescColuna)
    if row.ColunaObrigatoria==1: 
        return analitico_dataframe.union(analitico_null_dataframe), agrupado_dataframe.union(agrupado_null_dataframe), OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema)  
    if row.ColunaObrigatoria==0: 
        return analitico_dataframe, agrupado_dataframe, OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema) 

def IsTipo(dataframe, row, dic):


    if row.ColunaObrigatoria==1:
        analitico_null_dataframe, agrupado_null_dataframe, dic = IsNull(dataframe, row, dic)

    if row.Valores =='Inteiro':
        dataframe_tratado = spark.sql(f"""select Linha, 
                                        {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                                        case when cast(REPLACE(REPLACE({row.Coluna}, '.', ''), ',', '.') as int) is null then 'Valor não pode ser transformado para inteiro.'
                                        else 'Valor pode ser transformado para inteiro' end as Critica
                                        from viewdf
                                        where (({row.Coluna} is not null) and ({row.Coluna} <>''))
                                         and cast(REPLACE(REPLACE({row.Coluna}, '.', ''), ',', '.') as int) is null""")

        problema = 'Valores que deveriam ser Inteiros, não podem ser transformados para esse tipo de dado.'

    if row.Valores.split(';')[0] == 'Data':
        dic_data = {'dd/MM/yyyy': 'dd/MM/y', 'yyyy-MM-dd HH:mm:ss': 'yyyy-MM-dd HH:mm:ss'}
        formato = dic_data[row.Valores.split(';')[1]]
        dataframe_tratado = spark.sql(f"""select Linha, 
                                        {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                                        case when to_date({row.Coluna}, '{formato}') is null then 'A data foi enviada em um formato incorreto.'
                                             when to_date({row.Coluna}, '{formato}') <= '2022-01-01' then 'A data é muito antiga.'
                                             when to_date({row.Coluna}, '{formato}') >= add_months(current_date(), 1) then 'A data está no futuro.'
                                        else 'A data foi enviada em um formato correto.' end as Critica
                                from viewdf
                                 where (({row.Coluna} is not null) and ({row.Coluna} <>'')) and 
                                 (to_date({row.Coluna}, '{formato}') is null or 
                                 to_date({row.Coluna}, '{formato}') <= '2022-01-01' 
                                or to_date({row.Coluna}, '{formato}') >= add_months(current_date(), 1))""")
        
        problema = 'Valores que deveriam ser Data, não podem ser transformados para esse tipo de dado.'

    if row.Valores == 'Float':

        dataframe_tratado = spark.sql(f"""select Linha, {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                case when cast(REPLACE(REPLACE({row.Coluna}, '.', ''), ',', '.') as decimal(18,2)) is null then 'Não pode ser transformado para decimal.'
                                        else 'Valor pode ser transformado para decimal.' end as Critica 
                                         from viewdf
                                        where (({row.Coluna} is not null) and ({row.Coluna} <>'')) and
                                         cast(REPLACE(REPLACE({row.Coluna}, '.', ''), ',', '.') as decimal(18,2)) is null""")

        problema = 'Valores que deveriam ser número decimal, não podem ser transformados para esse tipo de dado.'

    if 'Texto' in row.Valores:
        num = int(row.Valores[len('Texto'):])
        dataframe_tratado = spark.sql(f"""select Linha, 
                                        {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                                        case when char_length({row.Coluna}) <= {num} then 'Campo texto menor ou igual que {num} caracteres.'
                                             when char_length({row.Coluna}) > {num} then 'Campo texto maior que {num} caracteres.'
                                        else 'Valor preenchido' end as Critica
                                        from viewdf
                                        where (({row.Coluna} is not null) and ({row.Coluna} <>'')) and char_length({row.Coluna}) > {num}""")

        problema = f'Campo texto maior que {num} caracteres.'

    nrows = dataframe_tratado.count()
    analitico_dataframe, agrupado_dataframe = OutputsDataframes(dataframe_tratado, str(row.DescColuna).replace(' ', ''))
    if row.ColunaObrigatoria==1: 
        return analitico_dataframe.union(analitico_null_dataframe), agrupado_dataframe.union(agrupado_null_dataframe), OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema)  
    if row.ColunaObrigatoria==0: 
        return analitico_dataframe, agrupado_dataframe, OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema) 

def ValoresIn(dataframe, row, dic):
    if row.ColunaObrigatoria==1:
        analitico_null_dataframe, agrupado_null_dataframe, dic = IsNull(dataframe, row, dic)

    dataframe_tratado = spark.sql(f"""select Linha, 
                                            {row.Coluna} as `{str(row.DescColuna).replace(' ', '')}`,
                                            case when {row.Coluna} in ('{"','".join(row.Valores.split(', '))}') then 'Valor esperado.'
                                            else 'Valor fora do esperado.' end as Critica 
                                            from viewdf as tb
                                            where (({row.Coluna} is not null) and ({row.Coluna} <>'')) and {row.Coluna} not in ('{"','".join(row.Valores.split(', '))}')""")

    nrows = dataframe_tratado.count()
    problema = f'Valores apresentados estão fora do esperado. Para a coluna {row.DescColuna} os valores esperados são {row.Valores}.'
    analitico_dataframe, agrupado_dataframe = OutputsDataframes(dataframe_tratado, str(row.DescColuna).replace(' ', ''))
    if row.ColunaObrigatoria==1: 
        return analitico_dataframe.union(analitico_null_dataframe), agrupado_dataframe.union(agrupado_null_dataframe), OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema)  
    if row.ColunaObrigatoria==0: 
        return analitico_dataframe, agrupado_dataframe, OutputString(agrupado_dataframe,nrows,  dic, row.DescColuna, problema)

def VariaveisPipeline():
    # Cria variaveis uteis durante o pipeline
    dic = {}

    schema_analitico = StructType([
        StructField("Coluna", StringType(), True),
        StructField("Linha", StringType(), True),
        StructField("Valor", StringType(), True),
        StructField("Critica", StringType(), True)
    ])

    schema_agrupado = StructType([
        StructField("Coluna", StringType(), True),
        StructField("Critica", StringType(), True),
        StructField("Valor", StringType(), True),
        StructField("Linhas com problemas (exemplo)", StringType(), True)
    ])

    analiticofinal = spark.createDataFrame([], schema_analitico)
    agrupadofinal = spark.createDataFrame([], schema_agrupado)
    return dic, analiticofinal, agrupadofinal

def Credential(server_sqlserver, bancodados_sqlserver, user_sqlserver, password_sqlserver):
    tabela_credenciais = spark.read \
            .format("jdbc") \
            .option("url", f'jdbc:sqlserver://{server_sqlserver};database={bancodados_sqlserver}') \
            .option("query", """select * from [monitor].[tblcredenciaisfaturamento] where ativo=1 and cliente='Kibon'""") \
            .option("user", user_sqlserver) \
            .option("password", password_sqlserver).load()
    return tabela_credenciais

def SqlServerQuery(server_sqlserver,bancodados_sqlserver,user_sqlserver, password_sqlserver, query):
    #Read Table from SQLServer
    tabela_validacoes = spark.read \
                            .format("jdbc") \
                            .option("url", f'jdbc:sqlserver://{server_sqlserver};database={bancodados_sqlserver}') \
                            .option("query", query) \
                            .option("user", user_sqlserver) \
                            .option("password", password_sqlserver) \
                            .load()
    return tabela_validacoes

def MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql, query):
      # Read from MySQL Table
    tabela_origem = spark.read\
                  .format("jdbc").\
                  option("url", f"jdbc:mysql://{server_mysql}/{bancodedados_mysql}")\
                  .option("driver", "com.mysql.jdbc.Driver")\
                  .option("query", query)\
                  .option("user", user_mysql)\
                  .option("password", password_mysql)\
                  .load()

    return tabela_origem

def outputemail(dic):
    string_final = ''
    #sempre coluna \n texto \n dataframe ...\n texto \n dataframe coluna
    if len(dic.keys()) == 1:
        string_final = '<br>' + 'Reporte'  + '<br>' + dic['Reporte'][0] + '<br>' + 'Validacao concluida com sucesso!'
        return string_final
    else:
        for string in dic.keys():
            if len(dic[string]) == 1:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>'
            if len(dic[string]) == 2:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>'
            if len(dic[string]) == 3:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>'
            if len(dic[string]) == 4:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>' + dic[string][3]
            if len(dic[string]) == 5:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>' + dic[string][3] +'<br>' + dic[string][4]
            if len(dic[string]) == 6:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>' + dic[string][3] +'<br>' + dic[string][4] +'<br>' + dic[string][5]
            if len(dic[string]) == 7:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>' + dic[string][3] +'<br>' + dic[string][4] + '<br>' + dic[string][5] +'<br>' + dic[string][6]
            if len(dic[string]) == 8:
                string_final =  string_final + '<br>' + string  + '<br>' + dic[string][0] + '<br>' + dic[string][1] + '<br>' + dic[string][2] + '<br>' + dic[string][3] +'<br>' + dic[string][4] + '<br>' + dic[string][5] +'<br>' + dic[string][6] +'<br>' + dic[string][7]

        return string_final 

def enviar_email(string_final, rowtabelaorigem, row_credenciais):
    smtp_server = "smtp.office365.com"
    port = 587
    sender_email = "no-reply@arker.com.br"
    password = "Wad95097"
    recipients = ["felipe.lima@neogrid.com"]
    # Criação da mensagem
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(recipients) 
    message["Subject"] = f'Testes Qualidade: {rowtabelaorigem.TipoArquivo} - {row_credenciais.Cliente}: {rowtabelaorigem.NomeOriginal}'
    
    # Corpo da mensagem
    body = string_final
    message.attach(MIMEText(body, "html"))

    # Conexão SSL segura com o servidor SMTP
    context = ssl.create_default_context() 
    with smtplib.SMTP(smtp_server, port) as server: 
        server.ehlo('mylowercasehost') 
        server.starttls(context=context) 
        server.ehlo('mylowercasehost') 
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, recipients, text)

def UploadBlob(dic, dataframe, row_tabelaorigem, row_credenciais, keyblob, tipo):

    dataatual = str(datetime.datetime.now())[:19].replace('-','').replace(' ', '_').replace(':', '')
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem.ContainerName};AccountKey={keyblob};EndpointSuffix=core.windows.net"
    writer = io.BytesIO()
    dataframe.toPandas().to_excel(writer,index=False)

    BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem.ContainerName,
                                                blob_name=f'{row_tabelaorigem.CaminhoOrigem}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx')\
            .upload_blob(writer.getvalue(), blob_type="BlockBlob", connection_timeout=1000)
    if 'Artefatos: \n' in dic.keys():
        dic['Artefatos: \n'] = dic['Artefatos: \n'] + [f'{tipo}: \n', create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual)]
        return dic
    if 'Artefatos: \n' not in dic.keys():
        dic['Artefatos: \n'] =  [f'{tipo}: \n', create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual)]
        return dic

def create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual):
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(days=7)
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem.ContainerName};AccountKey={keyblob};EndpointSuffix=core.windows.net"
    # Criar um serviço de cliente BlobServiceClient
    blob_client = BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem.ContainerName,
     blob_name=f'{row_tabelaorigem.CaminhoOrigem}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx')
    
    # Conectar ao container
    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=keyblob,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )
    print(f"{blob_client.url}?{sas_token}")
    return f"{blob_client.url}?{sas_token}"

def create_delta_table(df, row_tabelaorigem, key, tabela_validacoes, cliente, row_carga, linkedservice):

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    token_library = sc._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
    blob_sas_token = token_library.getConnectionString(linkedservice)

    

    spark.conf.set(
        'fs.azure.sas.%s.%s.blob.core.windows.net' % (row_tabelaorigem.ContainerName, row_tabelaorigem.ContainerName),
        blob_sas_token)

    path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/deltatable'

    oldColumns = pd.DataFrame(df.schema.names, columns=['Coluna'])
    newColumns = tabela_validacoes.filter(~tabela_validacoes['DescColuna'].isin(['ChaveMD5', 'Encoding', 'Header'])).select(['Coluna','DescColuna']).toPandas()
    newColumns = pd.concat([newColumns, pd.DataFrame([['Linha', 'Linha']], columns=['Coluna','DescColuna'])])
    df_aux = oldColumns.merge(newColumns, how='left', on='Coluna')
    newColumns= df_aux[~df_aux['Coluna'].isna()]['DescColuna'].tolist()
    oldColumns = df_aux[~df_aux['Coluna'].isna()]['Coluna'].tolist()
    df_new = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx],
                                                        newColumns[idx]),
                                                        range(len(oldColumns)),df)
    df_new.createOrReplaceTempView('viewdelta')
    df_to_sql = spark.sql(row_carga.ScriptDeltaTable.replace('\n', ' ').replace('\r', ' '))
    df_to_sql = df_to_sql.drop('Linha')
    df_to_sql = df_to_sql.drop('linha_')
    df_to_sql = df_to_sql.withColumn("DataInsercao", F.expr("current_timestamp() - INTERVAL 3 HOURS"))
    df_to_sql = df_to_sql.withColumn("ArquivoFonte", lit(row_tabelaorigem.TabelaOrigem))

    for i in df_to_sql.columns:

        df_to_sql = df_to_sql.withColumn(
            i, 
            when(col(i) == "", None).otherwise(col(i))
        )
    # spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/deltatable'
    df_to_sql.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)


def CargaSqlServer(cliente, row_tabelaorigem, row_carga, linkedservice, tabela_validacoes):

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    token_library = sc._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
    blob_sas_token = token_library.getConnectionString(linkedservice)

    spark.conf.set(
            'fs.azure.sas.%s.%s.blob.core.windows.net' % (row_tabelaorigem.ContainerName, row_tabelaorigem.ContainerName),
            blob_sas_token)

    path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/deltatable'

    df_delta = spark.read.format("delta").load(path)
    schema = df_delta.schema

    row_colunasobt = tabela_validacoes.filter(tabela_validacoes['Validacao']=='ColunasObrigatoriasTabela').collect()[0]
    new_fields = [
        StructField(field.name, field.dataType, nullable=False) if field.name in row_colunasobt.Valores.split(',') else field
        for field in schema.fields
    ]
    # Criar um novo schema com as modificações
    new_schema = StructType(new_fields)

    df_to_sql_new = spark.createDataFrame(df_delta.rdd, new_schema)

    url = "jdbc:sqlserver://" + row_carga.Server + ";" + "databaseName=" + row_carga.BancoDeDados + ";"


    df_to_sql_new = df_to_sql_new.withColumn(
        "DataInsercao", 
        to_timestamp(regexp_replace(col("DataInsercao"), r"\.\d+$", ""), "yyyy-MM-dd HH:mm:ss")
    )
    df_to_sql_new.coalesce(1)\
        .write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", f"{row_carga.TabelaDestinoCarga}diaria") \
        .option("user", row_carga.User) \
        .option("password", row_carga.Password) \
        .save()

def ExecuteScriptMySQL(row_credenciais, query):

    mydb = mysql.connector.connect(
    host=row_credenciais.MySQLServidor,
    user=row_credenciais.MySQLUsuario,
    password=row_credenciais.MySQLSenha,
    database=row_credenciais.MySQLBancoDeDados
    )

    mycursor = mydb.cursor()

    
    mycursor.execute(query)

    mydb.commit()
    mydb.close()
    
def ExecuteScriptSqlServer(row_carga, script):

    connection = ("Driver={ODBC Driver 18 for SQL Server};"
                    f"Server={row_carga.Server};"
                    f"Database={row_carga.BancoDeDados};"
                    f"UID={row_carga.User};"
                    f"PWD={row_carga.Password};")
    conn = pyodbc.connect(connection, autocommit=True) 
    try:
        cursor = conn.cursor()
        cursor.execute(script.replace('\n', ' ').replace('\r', ' '))
        return False
    except pyodbc.Error as e:
        # Captura e exibe a mensagem de erro
        sqlstate = e.args[0]  # Código do erro
        mensagem = e.args[1]  # Mensagem de erro detalhada
        print(f"Erro SQL: {sqlstate} - {mensagem}")
        return True
    finally:
        cursor.close()
        conn.close()


def Main():
    #credenciais sql server
    server_sqlserver = 'sqlarkerbigdata01.database.windows.net'
    bancodados_sqlserver='ArkerBIQA'
    user_sqlserver = 'ArkerBIQA'
    password_sqlserver = 'lrl1512Mj054*0LHb!Q'
    tabela_credenciais = Credential(server_sqlserver, bancodados_sqlserver, user_sqlserver, password_sqlserver)

    for row_credenciais in tabela_credenciais.collect():
        data = datetime.datetime.now() - timedelta(hours=3)
        datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
        print(datahorastr)
        print('\n')
        server_mysql = row_credenciais.MySQLServidor
        bancodedados_mysql = row_credenciais.MySQLBancoDeDados
        user_mysql = row_credenciais.MySQLUsuario
        password_mysql = row_credenciais.MySQLSenha
        keyblob = row_credenciais.SenhaBlob
        tipoarquivo = row_credenciais.TipoArquivo
        cliente = row_credenciais.Cliente
        linkedservice = row_credenciais.LinkedService
        query_naoiniciados_mysql = f"""SELECT * FROM tblpbiarquivoimport WHERE Id IN (SELECT MAX(Id) FROM tblpbiarquivoimport 
                  GROUP BY TabelaOrigem) AND STATUS = "NaoIniciado" AND Ativo = 1 AND TipoArquivo = '{tipoarquivo}' GROUP BY TabelaOrigem"""
        tabelas_origem = MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql, query_naoiniciados_mysql)
        query_validacao_sqlserver = f"""select * from [monitor].[ValidacaoArquivos] where cliente='{cliente}' and tipoarquivopbi='{tipoarquivo}'"""
        tabela_validacoes = SqlServerQuery(server_sqlserver,bancodados_sqlserver,user_sqlserver, password_sqlserver, query_validacao_sqlserver)
        if len(tabelas_origem.collect()) == 0:
            print('Sem arquivo! \n')
            continue
        for row_tabelaorigem in tabelas_origem.collect():
            dic, analiticofinal, agrupadofinal = VariaveisPipeline()
            query_atthomolog = f"""update tblpbiarquivoimport set status='Homologando',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
            ExecuteScriptMySQL(row_credenciais, query_atthomolog)
            print(f'Id do arquivo: {row_tabelaorigem.Id} Tipo de arquivo: {row_tabelaorigem.TipoArquivo}. Nome arquivo: {row_tabelaorigem.TabelaOrigem}\n')
            print('Atualizar Status - Homologando')
            data = datetime.datetime.now() - timedelta(hours=3)
            datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
            print(datahorastr)
            try: 
                del df
                del string_final
            except:
                pass
            try:
                row_encode = tabela_validacoes.filter(tabela_validacoes['Validacao']=='IsEncoding').collect()[0]
                dic, flag_encode_correto, encode_recebido = DetectFormatoEncoding(dic, row_tabelaorigem, row_encode, keyblob)
                #encode_recebido ='utf-8'
                print('Formato detectado. \n')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)

            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro detectFormatoEncoding: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                row_header = tabela_validacoes.filter(tabela_validacoes['Validacao']=='IsHeader').collect()[0]
                df, dic = LeArquivo(dic, row_tabelaorigem, row_header, tabela_validacoes, encode_recebido, cliente, keyblob, linkedservice)
                df.cache()
                print('Arquivo lido. \n')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro leitura arquivo: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                chavemd5 = tabela_validacoes.filter((tabela_validacoes['Validacao'] == 'IsChaveMD5') & 
                                            (tabela_validacoes['TipoArquivoPbi'] == tipoarquivo) &
                                             (tabela_validacoes['Cliente'] == cliente))
                if not chavemd5.isEmpty():
                    row_chavemd5 = chavemd5.collect()[0]
                    dic, dfchave_repetidas  = IsChaveUnica(df, row_chavemd5, dic, tabela_validacoes)
                    print('Chavemd5 validada\n')
                    data = datetime.datetime.now() - timedelta(hours=3)
                    datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                    print(datahorastr)
            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro chavemd5: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                print('Inicio processo de validação regras')
                print('\n')
                colunas_df = df.columns
                for row_validacao in tabela_validacoes.filter((~tabela_validacoes['Validacao'].isin(['IsChaveMD5', 'IsEncode', 'IsHeader', 'IsEncoding'])) &
                                                (tabela_validacoes['TipoArquivoPbi'] == tipoarquivo)&
                                             (tabela_validacoes['Cliente'] == cliente)).collect():
                    if row_validacao.Coluna not in colunas_df:
                        continue
                    print(row_validacao)
                    analitico, agrupado, dic = globals()[row_validacao.Validacao](df, row_validacao, dic)
                    analiticofinal = analiticofinal.union(analitico)
                    agrupadofinal = agrupadofinal.union(agrupado)
                print('Fim processo de validação regras. \n')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)

            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro Regras tabela validação: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                dic = UploadBlob(dic, agrupadofinal, row_tabelaorigem,row_credenciais, keyblob, 'Agrupado')
                dic = UploadBlob(dic, analiticofinal, row_tabelaorigem,row_credenciais, keyblob, 'Analítico')
                if (not chavemd5.isEmpty()) and (dfchave_repetidas is not None):
                    dic = UploadBlob(dic, dfchave_repetidas, row_tabelaorigem, row_credenciais, keyblob, 'ChaveMD5Repetidas')
                print('Upload de arquivos finalizado!')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)

            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro ao fazer upload de artefatos: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())

                break
                print('\n')    
            try:
                string_final = outputemail(dic)
                print('Corpo do email criado\n')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)

            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro criar corpo email: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')    
            try:
                enviar_email(string_final, row_tabelaorigem, row_credenciais) 
                print('Email enviado\n')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro enviar email: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                query_attprocessando = f"""update tblpbiarquivoimport set status='Processando',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_attprocessando)
                print('Atualizar Status - Processando')
                query_carga = f""" select * from monitor.CargaArquivos where cliente = '{cliente}' and tipoarquivo='{tipoarquivo}' """
                tabela_carga = SqlServerQuery(server_sqlserver,bancodados_sqlserver,user_sqlserver, password_sqlserver, query_carga)
                row_carga = tabela_carga.collect()[0]
                create_delta_table(df, row_tabelaorigem, keyblob, tabela_validacoes, cliente, row_carga, linkedservice)
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Delta table criada\n')
            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)

                print('Erro ao criar delta table: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')
            try:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Inicio processo carga')
                flagerro = ExecuteScriptSqlServer(row_carga, row_carga.ScriptPreCarga)
                if flagerro:
                    print('Atualizar Status - Erro')
                    query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                    pipelinename = 'Faturamento',
                                                                    dataultimaexecucao= '{datahorastr}',
                                                                    pipelinetriggername='Faturamento{cliente}',
                                                                    workspacename='prodsynapse01' 
                                                                    where id = {row_tabelaorigem.Id}"""
                    ExecuteScriptMySQL(row_credenciais, query_atterro)
                    break
                print('Tabela Truncada!')
                CargaSqlServer(cliente, row_tabelaorigem, row_carga, linkedservice, tabela_validacoes)
                print('Carga tabela staging completa!\n')
                flagerro = ExecuteScriptSqlServer(row_carga, row_carga.ScriptCarga)
                if flagerro:
                    print('Atualizar Status - Erro')
                    query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                    pipelinename = 'Faturamento',
                                                                    dataultimaexecucao= '{datahorastr}',
                                                                    pipelinetriggername='Faturamento{cliente}',
                                                                    workspacename='prodsynapse01' 
                                                                    where id = {row_tabelaorigem.Id}"""
                    ExecuteScriptMySQL(row_credenciais, query_atterro)
                    break
                print('Carga tabela final completa')
                print('Fim')
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Concluido')
                query_attconcluido = f"""update tblpbiarquivoimport set status='Concluido',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_attconcluido)
            except Exception as e:
                data = datetime.datetime.now() - timedelta(hours=3)
                datahorastr = data.strftime('%Y-%m-%d %H:%M:%S')
                print(datahorastr)
                print('Atualizar Status - Erro')
                query_atterro = f"""update tblpbiarquivoimport set status='Erro',
                                                                pipelinename = 'Faturamento',
                                                                dataultimaexecucao= '{datahorastr}',
                                                                pipelinetriggername='Faturamento{cliente}',
                                                                workspacename='prodsynapse01' 
                                                                where id = {row_tabelaorigem.Id}"""
                ExecuteScriptMySQL(row_credenciais, query_atterro)
                print('Erro ao fazer carga diaria: \n')
                print(e)
                print("Stack Trace:")
                print(traceback.format_exc())
                break
                print('\n')


# In[113]:


a = Main()


# In[ ]:




