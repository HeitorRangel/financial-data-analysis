# Financial Data Analysis

Pipeline automatizado para ingestão e visualização de dados do mercado financeiro. O projeto coleta dados da API do Yahoo Finance, armazena em um Data Lake Parquet particionado e fornece um dashboard interativo para análise.

## Componentes

### Ingestor
Serviço contínuo que busca dados de mercado para uma lista de ativos a cada 2 minutos. Os dados são processados para o formato Tidy e salvos com compressão Snappy, particionados por ano, mês e dia.

### Dashboard
Interface web construída com Streamlit e Plotly para visualizar os dados coletados. As funcionalidades incluem filtragem por ativo e data, indicadores chave (Preço, Variação, Volume) e gráficos interativos com médias móveis.

## Instalação

1. Clone o repositório
git clone https://github.com/HeitorRangel/financial-data-analysis.git

2. Instale as dependências
pip install -r requirements.txt

## Execução

### 1. Iniciar Ingestão
Execute o script de ingestão em um terminal. Mantenha essa janela aberta para manter a coleta de dados.
python ingestor.py

### 2. Iniciar Dashboard
Execute a aplicação do dashboard em um terminal separado.
streamlit run dashboard.py

## Estrutura de Dados
Os dados são armazenados localmente na seguinte estrutura:
datalake/ano=YYYY/mes=MM/dia=DD/market_data_HHMMSS.parquet
