import os
import gc
from tqdm import tqdm

ACTUAL_PATH = os.path.abspath(".")
DATA_PATH = os.path.join(ACTUAL_PATH, "analise_descritiva")

SQL_START = "INSERT INTO public.empresas_go (cnpj_basico, codigo_porte, porte, razao_social, codigo_natureza_juridica, natureza_juridica, qualificacao_do_responsavel, capital_social, ente_federativo_responsavel, opcao_pelo_simples, data_opcao_pelo_simples, data_exclusao_do_simples, opcao_pelo_mei, data_opcao_pelo_mei, data_exclusao_do_mei, cnpj, identificador_matriz_filial, descricao_matriz_filial, nome_fantasia, situacao_cadastral, descricao_situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, descricao_motivo_situacao_cadastral, nome_cidade_no_exterior, codigo_pais, pais, data_inicio_atividade, cnae_fiscal, cnae_fiscal_descricao, cnaes_secundarios, descricao_tipo_de_logradouro, logradouro, numero, complemento, bairro, cep, uf, codigo_municipio, codigo_municipio_ibge, municipio, telefone1, telefone2, fax, email, situacao_especial, data_situacao_especial) VALUES"

with open(os.path.join(DATA_PATH, 'empresas_go_inserts.sql'), encoding='utf-8') as f:
    # Read all the lines from the file and store them as a list
    lines = f.readlines()

flag = True
file_it = 1

for it, line in enumerate(tqdm(lines)):

    # Remove the trailing newline character
    line = line.rstrip()

    if(flag):
        line = line[:-1]
        flag = False
    elif(it >= 1000 and it % 1000 == 0):
        line = line.replace(SQL_START, ",", -1)
        flag = True
    else:
        line = line[:-1]
        line = line.replace(SQL_START, ",", -1)

    # Write the line to the output file
    if(it >= 100000 and it % 100000 == 1):
        file_it += 1
    with open(os.path.join(DATA_PATH, f'empresas_go_inserts_bulk_{file_it}.sql'), 'a', encoding='utf-8') as f:
        f.write(line)