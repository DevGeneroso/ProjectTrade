#importar as bibliotecas

import yfinance as yf
import pandas as pd
import numpy as np
import os.path
import telegram
import asyncio
pd.options.mode.chained_assignment = None

#Escolher o ativo
itub = yf.Ticker('BBAS3.SA')

#Escolher um intervalo de dados
itub_dia = itub.history(period='1d', interval='5m')

#pegar o preço de fechamento
itub_dia =itub_dia.Close

#transforma em dataframe
df_itub_dia = pd.DataFrame(itub_dia)

#reset index
df_itub_dia.reset_index(inplace=True)

#ultimo preço negociado
itub_dia_ultimo_preco = df_itub_dia.tail(1)

#Mudar o nome
itub_dia_ultimo_preco.rename(columns={'Datetime': 'data_pregao', 'Close': 'preco_fechamento'}, inplace=True)

#ajustar a data
itub_dia_ultimo_preco['data_pregao']=pd.to_datetime(itub_dia_ultimo_preco['data_pregao'],format='%Y-%m-%d')

#Usar o data frame historico e pegar apenas o preço de fechamento e data do pregão
if os.path.isfile('caixa.csv'):
  df_itau = pd.read_csv('caixa.csv', delimiter=',')
else:
  df = pd.read_csv('all_bovespa.csv', delimiter=',')
  df_itau= df[df['sigla_acao']== 'CXSE3']  
  df_itau = df_itau[['data_pregao', 'preco_fechamento']]

#Ajustar data  
df_itau['data_pregao']=pd.to_datetime(df_itau['data_pregao'],format='%Y-%m-%d')

#Retirar a ultima data
df_remove = df_itau.loc[(df_itau['data_pregao'] == pd. to_datetime('today').normalize())]

df_itau = df_itau.drop(df_remove.index)

#append data atual
df_itub_total = df_itau._append(itub_dia_ultimo_preco)

#ajustar as datas
df_itub_total['data_pregao']=pd.to_datetime(df_itub_total['data_pregao'], utc=True).dt.date

df_itub_total.to_csv('itau.csv', sep=',', index=False)

#calcular MACD

rapidaMME=df_itub_total.preco_fechamento.ewm(span=12).mean()
lentaMME=df_itub_total.preco_fechamento.ewm(span=26).mean()
MACD= rapidaMME - lentaMME
sinal=MACD.ewm(span=9).mean()

df_itub_total['MACD'] = MACD
df_itub_total['sinal'] = sinal

#ajuste do index e retirar o campo data pregão
df_itub_total = df_itub_total.set_index(pd.DatetimeIndex(df_itub_total['data_pregao'].values))
df_itub_total = df_itub_total.drop('data_pregao', axis=1)

#Criar cidugi oara verificar a compra e a venda
df_itub_total['flag'] = ''
df_itub_total['preco_compra'] = np.nan
df_itub_total['preco_venda'] = np.nan

for i in range(1, len(df_itub_total.sinal)):
    if df_itub_total['MACD'][i] > df_itub_total['sinal'][i]:
        if df_itub_total['flag'][i-1] == 'C':
            df_itub_total['flag'][i] ='C'
        else:
            df_itub_total['flag'][i] ='C'
            df_itub_total['preco_compra'][i] = df_itub_total['preco_fechamento'][i]

    elif df_itub_total['MACD'][i] < df_itub_total['sinal'][i]:
        if df_itub_total['flag'][i-1] =='V':
            df_itub_total['flag'][i] ='V'
        else:
            df_itub_total['flag'][i] ='V'
            df_itub_total['preco_venda'][i] = df_itub_total['preco_fechamento'][i]

#Verifica os 2 ultimos dias
hoje = df_itub_total.flag[-1]
ontem = df_itub_total.flag[-2]            

flag = hoje
preco_fechamento = round(df_itub_total.preco_fechamento.tail(1)[-1],2)

print(flag, preco_fechamento)


my_token = '6596599865:AAGXJ0WIhVUZ5oWVYhtn1DCl5DF9DZdEQes'
chat_id = '-4087539697'

async def envia_mensagem(msg, chat_id, token=my_token):
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=msg)

msg = f'CXSE3 (Caixa Seguridade), {flag} preço de fechamento : {preco_fechamento}'

async def envia_mensagem_se_necessario(msg, chat_id, my_token):
    if ontem != hoje:
        await envia_mensagem(msg, chat_id, my_token)

asyncio.run(envia_mensagem_se_necessario(msg, chat_id, my_token))

