from s001_download_zips_lai import baixa_zips
from s002_extrair_arquivos import unzip_lai
from s003_gerar_datasets import cria_datasets

pipeline = [baixa_zips,
            unzip_lai,
            cria_datasets
            ]

def run_pipeline():
    for task in pipeline:
        task()

if __name__ == '__main__':
    run_pipeline()