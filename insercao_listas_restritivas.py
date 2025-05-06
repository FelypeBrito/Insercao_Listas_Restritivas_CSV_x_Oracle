import os
import time
import shutil
import logging
from datetime import datetime
import math
import pandas as pd
import cx_Oracle
import chardet

# Configuração do banco
USUARIO_ORACLE = ""
SENHA_ORACLE = ""
NOME_SERVIDOR = ""
NOME_SERVICO = ""

PASTAS_TABELAS = {
    "/home/oracle/Analise/Lista_Restritivas/Ceis": "scd_listas_ceis",
    "/home/oracle/Analise/Lista_Restritivas/Cnep": "scd_listas_cnep",
    "/home/oracle/Analise/Lista_Restritivas/Acordos": "scd_listas_acordos_leniencia",
    "/home/oracle/Analise/Lista_Restritivas/Efeitos": "scd_listas_efeitos_leniencia",
    "/home/oracle/Analise/Lista_Restritivas/Cepim": "scd_listas_cepim"
}

# Log
logging.basicConfig(
    filename='processamento.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def tratar_valor(valor):
    """
    Retorna None se o valor for None, 'nan', string vazia, ou NaN (float).
    Caso contrário, retorna o valor original (sem alterações).
    """
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    if isinstance(valor, str) and valor.strip().lower() in ('', 'nan', 'none'):
        return None
    return valor

def conectar_banco():
    dsn = cx_Oracle.makedsn(NOME_SERVIDOR, port=1521, service_name=NOME_SERVICO)
    try:
        return cx_Oracle.connect(USUARIO_ORACLE, SENHA_ORACLE, dsn)
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Erro ao conectar ao banco: {e}")
        return None

def extrair_data(nome_arquivo):
    try:
        return datetime.strptime(nome_arquivo.split('_')[0], '%Y%m%d').strftime('%d/%m/%Y')
    except Exception as e:
        logging.warning(f"Erro ao extrair data do nome do arquivo '{nome_arquivo}': {e}")
        return None

def tratar_valor(valor, tipo):
    try:
        if tipo == "data":
            if pd.isna(valor) or str(valor).strip() in ["", "00/00/0000", "N/A", "-"]:
                return None
            return datetime.strptime(valor.strip(), "%d/%m/%Y").date()

        elif tipo == "valor":
            if pd.isna(valor):
                return None
            return float(str(valor).replace('.', '').replace(',', '.'))

        elif tipo == "limpar":
            if pd.isna(valor) or str(valor).strip().lower() in ["", "nan"]:
                return None
            return str(valor).strip()

        elif tipo == "notacao_cientifica":
            if valor is None or (isinstance(valor, float) and pd.isna(valor)):
                return None

            if isinstance(valor, str) and valor.isdigit():
                return valor.zfill(14)

            valor_str = str(valor).replace(',', '.').strip()

            if re.fullmatch(r"\d+(\.\d+)?([eE][+-]?\d+)?", valor_str):
                return str(int(float(valor_str))).zfill(14)

            return valor_str

        else:
            raise ValueError(f"Tipo de tratamento desconhecido: {tipo}")

    except Exception as e:
        logging.warning(f"Erro ao tratar valor '{valor}' como '{tipo}': {e}")
        return None

def detectar_codificacao(caminho_arquivo):
    with open(caminho_arquivo, 'rb') as f:
        resultado = chardet.detect(f.read(10000))
        return resultado['encoding'] or 'utf-8'

def mover_para_backup(caminho_origem):
    pasta_destino = os.path.join(os.path.dirname(caminho_origem), 'arquivos_lidos')
    os.makedirs(pasta_destino, exist_ok=True)
    shutil.move(caminho_origem, os.path.join(pasta_destino, os.path.basename(caminho_origem)))

def inserir_linha(tabela, row, cursor, nome_arquivo):
    if tabela == 'scd_listas_ceis':
        cursor.execute(f"""
            INSERT INTO {tabela} (
                tipo_de_pessoa,
                cpf_cnpj_sancionado,
                nome_informado_sancionador,
                razao_social_cadastro_receita,
                nome_fantasia_cadastro_receita,
                numero_do_processo,
                tipo_sancao,
                data_inicio_sancao,
                data_final_sancao,
                orgao_sancionador,
                uf_orgao_sancionador,
                origem_informacoes,
                data_origem_informacoes,
                data_publicacao,
                publicacao,
                detalhamento,
                abrag_def_decisao_judicial,
                fundamentacao_legal,
                descricao_fundamentacao_legal,
                data_transito_julgado,
                complemento_orgao,
                observacoes,
                ult_atualizacao,
                cadastro,
                codigo_sancao,
                nome_sancionado,
                esfera_orgao_sancionador,
                data_inclusao
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, TO_DATE(:8, 'DD/MM/RRRR'), TO_DATE(:9, 'DD/MM/RRRR'),
                :10, :11, :12, TO_DATE(:13, 'DD/MM/RRRR'), TO_DATE(:14, 'DD/MM/RRRR'),
                :15, :16, :17, :18, TO_DATE(:19, 'DD/MM/RRRR'), :20, :21, :22,
                TO_DATE(:23, 'DD/MM/RRRR'), :24, :25, :26, :27, SYSDATE
            )
        """, (
            tratar_valor(row.get('TIPO DE PESSOA'),'limpar'),
            row.get('CPF OU CNPJ DO SANCIONADO'),
            tratar_valor(row.get('NOME INFORMADO PELO ÓRGÃO SANCIONADOR'),'limpar'),           
            tratar_valor(row.get('RAZÃO SOCIAL - CADASTRO RECEITA'),'limpar'),
            tratar_valor(row.get('NOME FANTASIA - CADASTRO RECEITA'),'limpar'),
            row.get('NÚMERO DO PROCESSO'),
            tratar_valor(row.get('CATEGORIA DA SANÇÃO'),'limpar'),
            tratar_valor(row.get('DATA INÍCIO SANÇÃO'),'data'),
            tratar_valor(row.get('DATA FINAL SANÇÃO'),'data'),
            tratar_valor(row.get('ÓRGÃO SANCIONADOR'),'limpar'),
            tratar_valor(row.get('UF ÓRGÃO SANCIONADOR'),'limpar'),
            tratar_valor(row.get('ORIGEM INFORMAÇÕES'),'limpar'),
            tratar_valor(row.get('DATA ORIGEM INFORMAÇÃO'),'data'),
            tratar_valor(row.get('DATA PUBLICAÇÃO'),'data'),
            tratar_valor(row.get('PUBLICAÇÃO'),'limpar'),
            tratar_valor(row.get('DETALHAMENTO DO MEIO DE PUBLICAÇÃO'),'limpar'),
            tratar_valor(row.get('ABRAGÊNCIA DA SANÇÃO'),'limpar'),
            tratar_valor(row.get('FUNDAMENTAÇÃO LEGAL'),'limpar'),
            None,  # descricao_fundamentacao_legal
            tratar_valor(row.get('DATA DO TRÂNSITO EM JULGADO'),'data'),
            None,  # complemento_orgao
            tratar_valor(row.get('OBSERVAÇÕES'),'limpar'),
            nome_arquivo,
            tratar_valor(row.get('CADASTRO'),'limpar'),
            tratar_valor(row.get('CÓDIGO DA SANÇÃO'),'limpar'),
            tratar_valor(row.get('NOME DO SANCIONADO'),'limpar'),
            tratar_valor(row.get('ESFERA ÓRGÃO SANCIONADOR'),'limpar')
        ))

    elif tabela == 'scd_listas_cnep':
        try:
            cursor.execute(f"""
                INSERT INTO {tabela} (
                    tipo_pessoa,
                    cpf_cnpj_sancionado,
                    nome_informado_sancionador,
                    razao_social_cadastro_receita,
                    nome_fantasia_cadastro_receita,
                    numero_processo,
                    tipo_sancao,
                    data_inicio_sancao,
                    data_final_sancao,
                    orgao_sancionador,
                    uf_orgao_sancionador,
                    origem_informacoes,
                    data_origem_informacoes,
                    data_publicacao,
                    publicacao,
                    detalhamento,
                    valor_multa,
                    fundamentacao_legal,
                    descricao_fundamentacao_legal,
                    data_transito_julgado,
                    complemento_orgao,
                    observacoes,
                    ult_atualizacao,
                    cadastro,
                    codigo_sancao,
                    nome_sancionado,
                    abrang_sancao,
                    esfera_orgao_sancionador,
                    data_inclusao
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7,
                    TO_DATE(:8, 'DD/MM/RRRR'),
                    TO_DATE(:9, 'DD/MM/RRRR'),
                    :10, :11, :12,
                    TO_DATE(:13, 'DD/MM/RRRR'),
                    TO_DATE(:14, 'DD/MM/RRRR'),
                    :15, :16, :17, :18,
                    TO_DATE(:19, 'DD/MM/RRRR'),
                    :20, :21, :22, TO_DATE(:23, 'DD/MM/RRRR'), :24, :25, :26, :27,
                    :28,
                    SYSDATE
                )
            """, (
                tratar_valor(row.get('TIPO DE PESSOA'),'limpar'),
                row.get('CPF OU CNPJ DO SANCIONADO'),
                tratar_valor(row.get('NOME INFORMADO PELO ÓRGÃO SANCIONADOR'),'limpar'),
                tratar_valor(row.get('RAZÃO SOCIAL - CADASTRO RECEITA'),'limpar'),
                tratar_valor(row.get('NOME FANTASIA - CADASTRO RECEITA'),'limpar'),
                row.get('NÚMERO DO PROCESSO'),
                tratar_valor(row.get('CATEGORIA DA SANÇÃO'),'limpar'),
                tratar_valor(row.get('DATA INÍCIO SANÇÃO'),'data'),
                tratar_valor(row.get('DATA FINAL SANÇÃO'),'data'),
                tratar_valor(row.get('ÓRGÃO SANCIONADOR'),'limpar'),
                tratar_valor(row.get('UF ÓRGÃO SANCIONADOR'),'limpar'),
                tratar_valor(row.get('ORIGEM INFORMAÇÕES'),'limpar'),
                tratar_valor(row.get('DATA ORIGEM INFORMAÇÃO'),'data'),
                tratar_valor(row.get('DATA PUBLICAÇÃO'),'data'),
                tratar_valor(row.get('PUBLICAÇÃO'),'limpar'),
                tratar_valor(row.get('DETALHAMENTO DO MEIO DE PUBLICAÇÃO'),'limpar'),
                tratar_valor(row.get('VALOR MULTA'),'valor'),
                tratar_valor(row.get('FUNDAMENTAÇÃO LEGAL'),'limpar'),
                None,  # descricao_fundamentacao_legal
                tratar_valor(row.get('DATA DO TRÂNSITO EM JULGADO'),'data'),
                None,  # complemento_orgao
                tratar_valor(row.get('OBSERVAÇÕES'),'limpar'),
                nome_arquivo,
                tratar_valor(row.get('CADASTRO'),'limpar'),
                tratar_valor(row.get('CÓDIGO DA SANÇÃO'),'limpar'),
                tratar_valor(row.get('NOME DO SANCIONADO'),'limpar'),
                tratar_valor(row.get('ABRAGÊNCIA DA SANÇÃO'),'limpar'),
                tratar_valor(row.get('ESFERA ÓRGÃO SANCIONADOR'),'limpar')
            ))
        except cx_Oracle.DatabaseError as e:
            logging.error(f"Erro ao inserir linha de {nome_arquivo}: {e} | Linha: {row}")

    elif tabela == 'scd_listas_efeitos_leniencia':
        try:

            cursor.execute(f"""
                INSERT INTO {tabela} (
                    id_do_acordo,
                    efeito_do_acordo_de_leniencia,
                    complemento,
                    ult_atualizacao,
                    data_inclusao
                ) VALUES (
                    :1, :2, :3, TO_DATE(:4, 'DD/MM/RRRR'), SYSDATE
                )
            """, (
                tratar_valor(row.get('ID DO ACORDO'),'limpar'),
                tratar_valor(row.get('EFEITO DO ACORDO DE LENIENCIA'),'limpar'),
                tratar_valor(row.get('COMPLEMENTO'),'limpar'),
                nome_arquivo
            ))

        except cx_Oracle.DatabaseError as e:
            logging.error(f"Erro ao inserir linha de {nome_arquivo}: {e} | Linha: {row}")

    elif tabela == 'scd_listas_acordos_leniencia':
        cursor.execute(f"""
            INSERT INTO {tabela} (
                id_acordo,
                cnpj_sancionado,
                razao_social_cadastro_receita,
                nome_fantasia_cadastro_receita,
                data_inicio_acordo,
                data_fim_acordo,
                situacao_acordo_lenienica,
                data_informacao,
                numero_processo,
                termos_acordo,
                orgao_sancionador,
                ult_atualizacao,
                data_inclusao
            ) VALUES (
                :1, :2, :3, :4,
                TO_DATE(:5, 'DD/MM/RRRR'),
                TO_DATE(:6, 'DD/MM/RRRR'),
                :7,
                TO_DATE(:8, 'DD/MM/RRRR'),
                :9, :10, :11, TO_DATE(:12, 'DD/MM/RRRR'),
                SYSDATE
            )
        """, (
            tratar_valor(row.get('ID DO ACORDO'),'limpar'),
            tratar_valor(row.get('CNPJ DO SANCIONADO'),'limpar'),
            row.get('RAZÃO SOCIAL \u2013 CADASTRO RECEITA'),
            row.get('NOME FANTASIA \u2013 CADASTRO RECEITA'),
            tratar_valor(row.get('DATA DE INÍCIO DO ACORDO'),'data'),
            tratar_valor(row.get('DATA DE FIM DO ACORDO'),'data'),
            tratar_valor(row.get('SITUAÇÃO DO ACORDO DE LENIÊNICA'),'limpar'),
            tratar_valor(row.get('DATA DA INFORMAÇÃO'),'data'),
            tratar_valor(row.get('NÚMERO DO PROCESSO'),'limpar'),
            tratar_valor(row.get('TERMOS DO ACORDO'),'limpar'),
            tratar_valor(row.get('ÓRGÃO SANCIONADOR'),'limpar'),
            nome_arquivo
        ))

    elif tabela == 'scd_listas_cepim':
        cursor.execute(f"""
            INSERT INTO {tabela} (
                cnpj_entidade,
                nome_entidade,
                numero_convenio,
                orgao_concedente,
                motivo_impedimento,
                ult_atualizacao,
                data_inclusao
            ) VALUES (
                :1, :2, :3, :4, :5, TO_DATE(:6, 'DD/MM/RRRR'), SYSDATE
            )
        """, (
            row.get('CNPJ ENTIDADE'),
            tratar_valor(row.get('NOME ENTIDADE'),'limpar'),
            tratar_valor(row.get('NÚMERO CONVÊNIO'),'limpar'),
            tratar_valor(row.get('ÓRGÃO CONCEDENTE'),'limpar'),
            tratar_valor(row.get('MOTIVO DO IMPEDIMENTO'),'limpar'),
            nome_arquivo
        ))

    else:
        print(f"Tabela '{tabela}' não reconhecida. Linha ignorada.")


def processar_arquivos():
    conexao = conectar_banco()
    if not conexao:
        logging.error("Falha na conexão com o banco de dados.")
        return

    arquivos_processados = set()
    try:
        with conexao.cursor() as cursor:
            while True:
                for pasta, tabela in PASTAS_TABELAS.items():
                    if not os.path.exists(pasta):
                        continue

                    for arquivo in filter(lambda f: f.lower().endswith('.csv'), os.listdir(pasta)):
                        caminho = os.path.join(pasta, arquivo)
                        if caminho in arquivos_processados:
                            continue

                        logging.info(f"Processando arquivo '{arquivo}' para a tabela '{tabela}'.")
                        print(f"\nProcessando '{arquivo}' na tabela '{tabela}'...")
                        sucesso, erro = 0, 0

                        try:
                            encoding = detectar_codificacao(caminho)
                            dtype_dic = {
                                            
                                            'CNPJ_DO_SANCIONADO': str,
                                            'NUMERO_PROCESSO': str,
                                            'CPF_OU_CNPJ_DO_SANCIONADO': str,
                                            'CNPJ_ENTIDADE': str                            
                                        }

                            df = pd.read_csv(
                                            caminho,
                                            sep=';',
                                            encoding=encoding,
                                            dtype=dtype_dic,
                                            keep_default_na=False,
                                            na_filter=False
                                        )
                 
                            nome_formatado = extrair_data(arquivo)

                            for idx, row in df.iterrows():
                                try:
                                    inserir_linha(tabela, row, cursor, nome_formatado)
                                    sucesso += 1
                                except Exception as e:
                                    erro += 1
                                    logging.error(f"Erro na linha {idx + 1} ({arquivo}): {e} | Conteúdo: {row.to_dict()}")

                            conexao.commit()
                            mover_para_backup(caminho)
                            arquivos_processados.add(caminho)
                            logging.info(f"Arquivo '{arquivo}' processado com sucesso. Sucesso: {sucesso}, Erros: {erro}")
                            print(f"? Arquivo processado: {sucesso} inserções, {erro} erros.")
                        except Exception as e:
                            conexao.rollback()
                            logging.error(f"Falha ao processar '{arquivo}': {e}")
                            print(f"? Erro ao processar '{arquivo}': {e}")

                time.sleep(10)
    finally:
        conexao.close()

if __name__ == "__main__":
    processar_arquivos()
