from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
import pandas as pd
import glob

#Aqui se utiliza o módulo Glob, para abrir e concatenar os arquivos em um único DataFrame.
## É importante ressaltar que a função "glob.glob" recebe como parâmetro o endereço do diretório
## onde os arquivos estão armazenados. Como no meu caso os arquivos estão armazenados na mesma pasta em que o script,
## então o endereço se torna opcional.
dados = pd.DataFrame()
dflist = sorted(glob.glob("*.xlsx"))
for df in dflist:
    df=pd.read_excel(df)
    dados = pd.concat([dados,df])

#Aqui se seleciona apenas a coluna "Conteúdo", dos comentários cujo assunto é sintomas de COVID
conteudo = dados[dados['Temas'].str.contains("COR. Sintomas")]
conteudo = conteudo['Conteúdo']

#Aqui se cria uma lista de strings, com cada palavra de cada comentário
words = []
for line in conteudo:
    line = str(line)
    line = word_tokenize(line)
    words.extend(line)

#Aqui as pontuações são removidas e as palavras postas todas em letras minúsculas,
# para que a não haja diferenciação entre maiúsculas e minúsculas 
palavras_sem_pontuacao = [palavra.lower() for palavra in words if palavra.isalnum()]

#Finalmente, se calcula o número de ocorrências individuais de cada palavra
frequencia = FreqDist(palavras_sem_pontuacao)

#Aqui estão armazenadas as palavras com apenas uma ocorrência
aparecem_uma_vez = frequencia.hapaxes()
print(aparecem_uma_vez)

#Aqui estão armazenadas as palavras mais comuns,
# o método most_common aceita um número como parâmetro, 
#para determinar quantas palavras mostrar,
# no caso, temos o Top 10 palavras mais frequentes
mais_comuns = frequencia.most_common(10)
print(mais_comuns)