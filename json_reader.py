import os
import json


def carrega_arquivo_json(arquivo):
    with open(arquivo, 'r') as arquivo:
        dados = json.load(arquivo)
    return dados