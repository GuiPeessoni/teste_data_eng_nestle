# Teste Técnico Data Engineer Nestlé

![Teste Data Engineer Nestlé]( https://img.shields.io/badge/Teste%20Datalab%20Nestlé-green?style=for-the-badge&)
![Databricks]( https://img.shields.io/badge/Plataforma%20de%20ETL-grey?style=for-the-badge&logo=databricks)
![Conteiner]( https://img.shields.io/badge/Virtualização-blue?style=for-the-badge&logo=docker)
![Airflow]( https://img.shields.io/badge/Scheduler-grey?style=for-the-badge&logo=apacheairflow)
![Dataviz]( https://img.shields.io/badge/Dataviz-cyan?style=for-the-badge&logo=looker)


----
Este repositório tem a finalidade de documentar o teste técnico de Engenheiro de Dados para a Nestlé.

----
# Estrutura do projeto
```
.
├── Base_Files
│   ├── input_files
│   │   ├── base_clientes.json
│   │   ├── base_order.yaml
│   │   └── base_produtos.csv
│   └── output_files
│       ├── clientes.csv.csv
│       ├── order_clientes.csv
│       ├── order_item_products.csv
│       └── products.csv
├── ETL_Codes
│   ├── Bronze_to_Silver.py
│   ├── Landing_to_Bronze.py
│   ├── Silver_to_Gold.py
│   └── functions
│       └── funcs.py
├── Makefile
├── README.md
└── airflow
    ├── dags
    │   └── dag.py
    ├── docker-compose.yaml
    ├── logs
    └── plugins
```

# Arquivos iniciais
Os arquivos que foram disponibilizados pelo recruiter estão localizados em `Base_Files/input_files`

Afim de conhecer a base, antes do procesamento foi realizado ajustes nos documentos, como relatado no bloco abaixo.

```

json:
- Linha 43 tinha uma "\" indevida
- Linha 113 Faltava a ","
- Linha 179 faltava uma " fechando a string
- Linha 293 Faltava a ","


yaml:
- Linha 53, faltava um ":" referenciando o valor
- Linha 321506, faltava um "-" referenciando o inicio do client_id
- Linha 361732, continha um "z" no product_id
- Linha 716006, faltava um ":" abrindo o dict dos itens
- Linha 716997, adicionado um ".0" para conversão ao Float

csv
- Linha 1282, foi trocado a "|" pelo separador padrão do arquivo
- Linhas 38 e 606, foi trocado a "," pelo separador padrão do arquivo 

```


# Desenvolvimento

Todo o desenvolvimento foi realizado dentro do Databricks, utilizando um DBR 13.3 com Unity Catalog habilitado.
O Unity Catalog é um Open Source que tem a finalidade de gerenciar as tabelas que estão dentro do catálogo de dados.

O Databricks já tem algumas Libs pré instaladas (o que dispensa o uso de um requirements.txt) ou outra forma de gerenciameto de dependências

Dentro do Databricks, a estrutura dos códigos segue o que está na pasta `ETL_Codes`.



# Orquestração

A Orquestração do pipeline é realizada dentro do Apache Airflow, aonde dentro da pasta `airflow` temos o Docker compose para criar um docker local que roda o airflow na sua versão 2.7.3
A Execução do Notebook Databricks é realizada através do operador `DatabricksSubmitRunOperator` um provider do airflow para executar este tipo de job, usando a API do Databricks

# Output
No ETL `silver_to_gold.py` alem de salvar a tabela no formato Delta, foi também criado um arquivo no formato .csv, que será o arquivo que será utilizado de base no Dashboard.


# Dashboard
Será criado um Dashboard contendo as principais metricas de cliente, pedidos e produtos, utilizando a plataforma do `Google Looker Studio`, utilizando os dados acima mencionados!

[Dashboard](https://lookerstudio.google.com/reporting/ea1e3eb9-7dd4-466d-b959-20a4813449c6)


