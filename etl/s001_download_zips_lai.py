import requests
from datetime import date
import shutil
import os
import time


ANO_INICIO=2015
FORMATO='csv'
PREFIXO_ACESSO = "Pedidos"
PREFIXO_RECURSO = "Recursos_Reclamacoes"
ZIP_DATA_DIR=os.path.join(os.path.abspath('.'), 'zips')
NOME_ARQUIVO_FILTRADO = 'Arquivos_{formato_arquivo}_{ano}.zip'
URL_FILTRADOS = "https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR_Filtrado/{nome_arquivo}"
ANOS = range(ANO_INICIO, int(date.today().strftime("%Y"))+1)


def busca_textos_por_anos(anos, localizacao, formato_arquivo='csv'):
    """
    anos: Lista de anos para download.
    localizacao: Onde salvar os arquivos
    formato_arquivo: formatos xml|csv
    """
    
    for year in anos:
        nome_arquivo  = NOME_ARQUIVO_FILTRADO.format(formato_arquivo=formato_arquivo, ano=str(year))
        url = URL_FILTRADOS.format(nome_arquivo=nome_arquivo)
        try:
            response = requests.get(url, stream=True)
            with open(os.path.join(localizacao, nome_arquivo), 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)

            print(f"Arquivo {nome_arquivo} baixado de {url}")
        except Exception as e:
            print(e)
            print(f"Error baixando arquivo {nome_arquivo} da url {url}")

        time.sleep(2)

def baixa_zips():
    busca_textos_por_anos(anos=ANOS, localizacao=ZIP_DATA_DIR, formato_arquivo=FORMATO)

if __name__ == '__main__':
    baixa_zips()
    
