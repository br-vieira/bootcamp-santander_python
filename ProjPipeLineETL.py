'''===============================================================================================================================
PROJETO PIPELINE ETL COM PYTHON

Sobre:
O Projeto Pipeline ETL com Python é um desafio prático para o Bootcamp Santander - Ciência de Dados com Python da DIO.

Objetivo:
Criar um pipeline ETL utilizando a linguagem de programação Python

Projeto:
Partindo de um arquivo .csv, o qual contém os dados dos usuários de um banco, o programa irá extrair os dados e avaliar o perfil
econômico do usuário com base na sua renda e idade. Feita a avaliação, o programa irá oferecer um limite de crédito de acordo com
o perfil avaliado e irá gerar um novo arquivo no qual conterá, alem dos dados primários, a sugestão de crédito de acordo com a
avaliação.
=================================================================================================================================='''

import pandas as pd


'''===============================================================================================================================
EXTRACT

Primeiro vamos extrair os dados dos clientes do arquivo 'clientes.csv' e alocar os dados dentro da variável 'arquivo'.
=================================================================================================================================='''

arquivo = pd.read_csv("clientes.csv",names=['id','cliente','idade','renda'])



'''===============================================================================================================================
TRANSFORM

Nessa etapa iremos realizar a avaliação de crédito do cliente da seguinte forma:
    -> Clientes na faixa etária de 18 até 23 anos, terá disponivel um limite crédito de até 20% da sua renda;
    -> Clientes na faixa etária de 24 até 28 anos, terá disponivel um limite crédito de até 50% da sua renda;
    -> Clientes na faixa etária de 29 até 35 anos, terá disponivel um limite crédito de até 120% da sua renda;
    -> Clientes na faixa etária acima dos 35 anos, terá disponivel um limite crédito de até 200% da sua renda;


Para calcular os limites de crédito com base na idade e renda de cada cliente, vamos criar uma função 'limite_cred' que irá
retornar a tabela incial com uma nova coluna chamada 'limite de credito' a qual conterá o valor do limite sugerido.

Vamos criar uma variável chamada 'tabela', a qual será a cópia da variável 'arquivo', com o intuito de fazer alterações sem que os
dados originais sejam afetados.
=================================================================================================================================='''

def limite_cred(df):

  #df['lim_cred'] = 0
  valores = []

  for index, row in df.iterrows():
    if row['idade'] >= 18 and row['idade'] <= 23:
     calc = row['renda'] * 0.2
     valores.append(f'Nossa sugestão de limite de crédito para o cliente é de R${calc:.2f}')

    elif row['idade'] > 23 and row['idade'] <= 28:
     calc = row['renda'] * 0.5
     valores.append(f'Nossa sugestão de limite de crédito para o cliente é de R${calc:.2f}')

    elif row['idade'] > 28 and row['idade'] <= 35:
     calc = row['renda'] * 1.2
     valores.append(f'Nossa sugestão de limite de crédito para o cliente é de R${calc:.2f}')

    else:
     calc = row['renda'] * 2.0
     valores.append(f'Nossa sugestão de limite de crédito para o cliente é de R${calc:.2f}')

  df['limite de credito'] = valores

  return df


tabela = arquivo.copy()
limite_cred(tabela)



'''============================================================================================================================================
LOAD

Com a nossa tabela modificada, já constando as sugestões de limite de crédito para cada cliente do banco, iremos exportar esta tabela
com o nome "arquivo_final.csv", o qual será o nosso arquivo a ser carregado.
============================================================================================================================================'''

tabela.to_csv('arquivo_final.csv',index=False)
