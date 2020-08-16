from urllib.request import urlopen
from pymongo import MongoClient
from bson.son import SON
import pandas as pd
import random
import json
import re

def item_infos(item : dict):
    #Esta função concatena as informações relevantes sobre um determinado item em uma compra
    item_submit = {}
    item_submit['valor'] = item.get('vr_estimado')
    item_submit['quantidade'] = item.get('qt_material_alt')
    if item.get('ds_tipo_fornecedor_vencedor') == 'PJ':
        item_submit['cnpj_fornecedor'] = item.get('nu_cnpj_vencedor')
    if item.get('ds_tipo_fornecedor_vencedor') == 'PF':
        item_submit['cpf_fornecedor'] = item.get('nu_cpf_vencedor')
    return item_submit

def extrai_infos(item : dict):
    #Esta função extrai os códigos das compras listadas
    regex = re.compile('[0-9]+')
    compra_id =item['_links']['self']['href']
    compra_id = str(regex.search(compra_id)[0])
    return compra_id

def escolhe_compras(parametro_de_busca : str, codigo_parametro : str, ano: str, quantidade_a_selecionar : int) :
    #O desafio em questão, fala sobre escolher ao menos 10 compras feitas pela Universidade Federal do Ceará(UFC),
    #no ano de 2019. Essa função, permite que o usuário determine o parâmetro utilizado para fazer a busca
    #das compras, permite determinar o ano em questão, e também permite determinar quantas compras devem ser selecionadas.
    url = f"http://compras.dados.gov.br/compraSemLicitacao/v1/compras_slicitacao.json?{parametro_de_busca}={codigo_parametro}&dt_ano_aviso_licitacao={ano}"
    resposta = urlopen(url)
    resposta = resposta.read()
    compras = pd.read_json(resposta)
    info_compras = list(compras.at['compras', '_embedded'])

    #Esta função retorna uma lista contendo apenas os códigos relativos
    #às compras selecionadas, para que possam ser utilizados na busca por
    #mais informações.
    compras = random.choices(info_compras, k=quantidade_a_selecionar)
    lista_de_compras = []
    for compra in compras:
        lista_de_compras.append(extrai_infos(compra))
    return lista_de_compras 

def compra_insert(codigo_compra : str):
    #Recebe o código de uma determinada compra e realiza uma pesquisa sobre seus itens
    url_itens = f'http://compras.dados.gov.br/compraSemLicitacao/doc/item_slicitacao/{codigo_compra}/itens.json'
    resposta_itens = urlopen(url_itens)
    resposta_itens= resposta_itens.read()
    itens_compra = pd.read_json(resposta_itens)
    
    itens_na_compra = []
    fornecedores = []
    lista_itens = list(itens_compra.at['compras', '_embedded'])
    
    #Aqui, para cada item em uma determinada compra, são agregadas informações sobre preço e 
    # quantidade e sobre os forncedores
    for item in lista_itens:
        infos_item = item_infos(item)
        if 'cnpj_fornecedor' in infos_item:
            url_fornecedor = f'http://compras.dados.gov.br/fornecedores/v1/fornecedores.json?cnpj={infos_item.get("cnpj_fornecedor")}'
            resposta_fornecedor = urlopen(url_fornecedor)
            resposta_fornecedor = resposta_fornecedor.read()
            info_fornecedor = json.loads(resposta_fornecedor.decode('utf-8'))
            info_fornecedor = info_fornecedor['_embedded']['fornecedores'][0]
            fornecedores.append(info_fornecedor['nome'])
            
        if 'cpf_fornecedor' in infos_item:
            url_fornecedor = f'http://compras.dados.gov.br/fornecedores/v1/fornecedores.json?cpf={infos_item.get("cpf_fornecedor")}'
            resposta_fornecedor = urlopen(url_fornecedor)
            resposta_fornecedor = resposta_fornecedor.read()
            info_fornecedor = json.loads(resposta_fornecedor.decode('utf-8'))
            info_fornecedor = info_fornecedor['_embedded']['fornecedores'][0]
            fornecedores.append(info_fornecedor['nome'])
        itens_na_compra.append(infos_item)
        
        for item in itens_na_compra:
            valor_total = 0
            valor_total += int(item['valor'])
    #Esta função retorna um dicionário, que contém todas as informações relevantes para a tarefa,
    #em formato pronto para ser submetido no banco de dados (no formato aceito pelo MongoDB)     
    dict_para_submissao =  {'codigo_da_compra': codigo_compra, 'valor': valor_total, 'quantidade_de_itens': len(itens_na_compra), 'fornecedores': fornecedores }
    return dict_para_submissao


#Aqui se inicializa a base de dados e a coleção que irá armazenar as informações sobre as compras
##Vale ressaltar que por questões de conveniência a base de dados utilizadas aqui foi criada
##em um servidor local
base = MongoClient()
base_compras = base.test_database
info_compras = base_compras.test_collection

#Aqui se coleta as informações sobre as compras e as organiza em dicionários,
#pare serem inseridos na base de dados
lista_de_compras = escolhe_compras('co_uasg', '153045', '2019', 10)
lista_de_compras = [compra_insert(compra) for compra in lista_de_compras]

#Este método recebe uma lista de dicionários,
#e insere cada dicionário com informações sobre as compras na base de dados
info_compras.insert_many(lista_de_compras)

'''
A partir daqui, comecei a ter problemas de conexão com a API, então a função foi desenvolvida a partir de 
exemplos fictícios para simular o output real do código acima. Dessa maneira, apesar de o algorítimo
estar consistente com o que foi experimentado, não tive a oportunidade de lidar com eventuais particularidades
inerentes do dados fornecidos pelo servidor. 
'''


def calcula_infos_compras(collection):
    #Esta função recebe uma collection do MongoDB, e realiza uma query baseada nos parâmetros do desafio.
    pipeline_fornecedor_mais_frequente = [{"$unwind": "$fornecedores"},{"$group": {"_id": "$fornecedores", "count": {"$sum": 1}}},{"$sort": SON( [("count", -1), ("_id", -1)] ) } ]
    pipeline_valor_medio_compra = [{"$group": {"_id": None, "Médio": {"$avg": "$valor"}}} ]
    pipeline_valor_maximo_compra = [{"$group": {"_id": None, "Máximo": {"$max": "$valor"}}} ]
    pipeline_valor_minimo_compra = [{"$group": {"_id": None, "Mínimo": {"$min": "$valor"}}} ]
    pipeline_quantidade_total_itens = [{"$group": {"_id": None, "Total": {"$sum": "$quantidade_de_itens"}}} ]
    pipeline_quantidade_media_itens = [{"$group": {"_id": None, "Média": {"$avg": "$quantidade_de_itens"}}} ]

    fornecedor_mais_frequente = list(collection.aggregate(pipeline_fornecedor_mais_frequente))
    fornecedor_mais_frequente = fornecedor_mais_frequente[0]["_id"]
    valor_medio_compra = list(collection.aggregate(pipeline_valor_medio_compra))[0]
    valor_maximo_compra = list(collection.aggregate(pipeline_valor_maximo_compra))[0]
    valor_minimo_compra = list(collection.aggregate(pipeline_valor_minimo_compra))[0]
    quantidade_total_de_itens = list(collection.aggregate(pipeline_quantidade_total_itens))[0]
    quantidade_media_itens = list(collection.aggregate(pipeline_quantidade_media_itens))[0]

    #A função retorna uma lista de dicionários, onde cada dicionário é
    #o resultado de uma das queries feitas
    return [fornecedor_mais_frequente, valor_medio_compra, valor_maximo_compra, valor_minimo_compra, quantidade_total_de_itens, quantidade_media_itens]

resultado = calcula_infos_compras(info_compras)

#Aqui apenas uma formatação com maior legibilidade
#para exibir os resultados.
print(f'Forncedor mais frequente : {resultado[0]}\n', 
f'Valor médio das compras (R$) : {resultado[1].get("Médio")}\n',
f'Valor máximo das compras (R$) : {resultado[2].get("Máximo")}\n', 
f'Valor mínimo das compras (R$) : {resultado[3].get("Mínimo")}\n', 
f'Quantidade total de serviços e materiais contratados : {resultado[4].get("Total")}\n', 
f'Quantidade média de serviços e materiais contratados : {resultado[5].get("Média")}')
