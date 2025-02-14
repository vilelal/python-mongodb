#Importando as bibliotecas
import pandas as pd
import pyodbc

import pymongo
from pymongo import MongoClient

#Lendo o arquivo csv e o transformando em um DataFrame
nomes= pd.read_csv('nomes.csv')
df_nome=pd.DataFrame(nomes)

#Criando uma tabela(NomeCompleto) a partir das tabelas "Primeiro Nome" e "Sobrenome"
df_nome['NomeCompleto']= df_nome['Primeiro Nome']+" "+ df_nome['Sobrenome']

#Criando uma variável com os valores filtrados. Por exemplo, a primeira variável apenas mostrará informações das pessoas com sexo feminino
feminino_df= df_nome.loc[df_nome['Sexo']=='Feminino']
estadoSP_df= df_nome.loc[df_nome['Estado']=='SP']

#Estabelecendo a conexão com o banco de dados SQL Server
SERVER = 'DESKTOP-RUTJMII'
DATABASE = 'informacao'
USUARIO = 'sa'
SENHA = 'pe20082001'

def inserir_dados_Sql(df,tabela):
    
    #String responsável pela conexão
    string_conexao = f'DRIVER={{SQL Server Native Client 11.0}};SERVER={SERVER};DATABASE={DATABASE};UID={USUARIO};PWD={SENHA}'

    conexao= pyodbc.connect(string_conexao)
    cursor = conexao.cursor()

    #Código responsável pela implementação da tabela feminino_df na base de dados 
    for index, row in feminino_df.iterrows():
        #Comando em sql para a inserção de informações.
        sql = "INSERT INTO Tabela_Feminino (PrimeiroNome, Sobrenome, Idade, Estado, Sexo, NomeCompleto) VALUES (?, ?, ?, ?, ?, ?)"
    
        #Comando para executar
        cursor.execute(sql, tuple(row))
        cursor.commit()
    print("Informações insiridas com sucesso")

def inserir_dados_mongodb(df,collection):
    #Estabelecendo conexão com o Banco de dados NoSQL
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    print("Conexão realizada com o banco de dados efetuada com sucesso")


    db=client['banco']
    collection = db["Informacao"]

    #Transformando o DataFrame "estadosSP_df" em um dicionário, para que o formato do dado esteja de acordo com o banco de dados
    dados = df_nome.to_dict('records')
    collection.insert_many(dados)
    print("Conexão estabelecida")

