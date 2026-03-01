
import os
import zipfile
import re

ZIP_DATA_DIR=os.path.join(os.path.abspath('.'), 'zips')
CSV_DATA_DIR=os.path.join(os.path.abspath('.'), 'csvs')
XML_DATA_DIR=os.path.join(os.path.abspath('.'), 'xmls')

def unzip_arquivos(zip_dir, csv_dir):
    #https://stackoverflow.com/questions/3451111/unzipping-files-in-python

    pattern = re.compile(".*\.zip")
    files = os.listdir(zip_dir)
    for file in files:
        if os.path.isfile(os.path.join(zip_dir, file)) and pattern.match(file.lower()) != None:    
            arquivo = os.path.join(zip_dir, file)
            with zipfile.ZipFile(arquivo,"r") as arquivo_zip:
                print(f'descompactando {arquivo}')
                arquivo_zip.extractall(csv_dir)


def unzip_lai():
    unzip_arquivos(zip_dir=ZIP_DATA_DIR, csv_dir=CSV_DATA_DIR)


if __name__ == '__main__':
    unzip_lai()
    