ANP Fuel Sales ETL Test
=======================

Este repositório representa o desenvolvimento de um pipeline de dados que busca informações em tabelas dinâmicas de um relatório consolidado da ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)

## Ferramentas utilizadas 

As seguintes ferramentas foram utilizadas no desenvolvimento do pipeline:
* Python
* Airflow
* Docker

## Premissas 

Antes de iniciar o desenvolvimento, foi efetuado uma correção da planilha de dados com os relatórios dinâmicos fornecidos pela ANP, onde as tabelas dinâmicas não estavam com seus totais batendo com os valores mês a mês e ano a ano. 

## Desenvolvimento

Todo o fluxo de dados foi idealizado utilizando camadas, tais como:

* RAW (dados brutos)
* TRUSTED (dados normalizados em relação a nome de campos e tipos)
* REFINED (dados agregados e sumarizados)

Foram desenvolvidos métodos python para cada etapa, tendo como a saída de cada script um arquivo do tipo PARQUET. Todos os métodos foram agrupados em uma lib python chamada `utils.py` e reutilizada dentro do pipeline desenvolvido na ferramenta `AIRFLOW`.

Todas as saídas em arquivo `PARQUET` foram salvas na estrutura de pastas do `AIRFLOW`, especificamente na pasta `sink`, conforme imagem abaixo:

![alt text](/images/airflow_structure.png)

O ambiente de execução do pipeline foi desenvolvido com a ferramenta `AIRFLOW` e implementada utilizando o `Docker` como forma de distribuição.

Antes de inicializar o ambiente faça o `BUILD` do ambiente docker através do comando:

`sudo docker build -t "appdev:Dockerfile" .`

Após efetuar o `BUILD` utilize o próximo comando para inicializar o ambiente `AIRFLOW`:

`sudo docker-compose up`

Para parar a execução do ambiente utilize o comando:

`sudo docker-compose down`

Com o ambiente `AIRFLOW` inicializado, efetue o login na tela inicial utilizando as credenciais:

* username: airflow
* password: airflow

![alt text](/images/airflow1_login.png)

Após o login, será direcionado para a tela principal do `AIRFLOW` com uma única DAG desenvolvida para o processamento do pipeline da ANP, com o nome de `my_dag_anp`.

![alt text](/images/airflow1.png)


Clique na DAG `my_dag_anp` e em seguida clique na barra de ferramentas em `Graph`. Será apresentado todo o pipeline seguindo as etapdas `RAW`, `TRUSTED` e `REFINED`.

![alt text](/images/airflow2.png)


### Observação: As últimas duas tasks do pipeline, `rfn_sales_diesel_check_totals` e `rfn_sales_oil_derivative_check_totals` tem como objetivo validar se os totais das tabelas dinâmicas batem com os valores carregados pelo pipeline.