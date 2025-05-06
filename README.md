
# 🚀 Script de Processamento de Listas Restritivas

Este projeto tem como objetivo processar e inserir automaticamente dados de listas restritivas (CEIS, CNEP, Acordos de Leniência, Efeitos, CEPIM) em um banco de dados Oracle.

## 📁 Estrutura de Pastas Monitoradas

O script monitora as seguintes pastas:

- `/home/oracle/Analise/Lista_Restritivas/Ceis` → Tabela: `scd_listas_ceis`
- `/home/oracle/Analise/Lista_Restritivas/Cnep` → Tabela: `scd_listas_cnep`
- `/home/oracle/Analise/Lista_Restritivas/Acordos` → Tabela: `scd_listas_acordos_leniencia`
- `/home/oracle/Analise/Lista_Restritivas/Efeitos` → Tabela: `scd_listas_efeitos_leniencia`
- `/home/oracle/Analise/Lista_Restritivas/Cepim` → Tabela: `scd_listas_cepim`

## 🧠 Funcionalidades

- Detecta a codificação dos arquivos CSV com `chardet`
- Trata tipos de dados (datas, valores monetários, strings nulas, notação científica)
- Extrai data do nome do arquivo
- Move arquivos processados para uma pasta `arquivos_lidos`
- Insere os dados nas tabelas corretas do Oracle
- Gera logs detalhados em `processamento.log`

## 🛠️ Requisitos

- Python 3.8+
- Bibliotecas:
  - pandas
  - cx_Oracle
  - chardet
  - math
  - logging
  - shutil
  - os
  - datetime

## ⚙️ Configuração do Banco

A conexão é feita usando:

```python
USUARIO_ORACLE = ""
SENHA_ORACLE = ""
NOME_SERVIDOR = ""
NOME_SERVICO = ""
```

> Certifique-se de que o Oracle Client esteja instalado e configurado corretamente.

## 📝 Logs

Todos os passos, incluindo erros, são registrados em `processamento.log`.

## 📌 Observações

- O script foi modularizado para facilitar a manutenção.
- O tratamento dos dados é personalizado por tabela.
- Os nomes das colunas no CSV devem estar de acordo com os nomes esperados nos dicionários do código.

---

Criado com 💡 para automação de processos de conformidade.
