# Financial Data Analysis (Data Lake)

Pipeline automatizado para ingestão, processamento e visualização de dados do mercado financeiro brasileiro (B3). O projeto foi desenhado para operar em ambientes de nuvem (Cloud Computing, como AWS EC2), coletando dados de forma resiliente, armazenando-os em um Data Lake estruturado e exibindo análises em um painel interativo.

## Arquitetura e Componentes

### 1. Ingestor (ETL)
Serviço em segundo plano (background) responsável pela extração e transformação contínua dos dados.
* Fonte de Dados: Consome a API da Brapi (brapi.dev), contornando bloqueios de IP comuns a servidores de nuvem.
* Smart Scheduling (Agendamento Inteligente): O script verifica o fuso horário e executa a extração apenas em dias úteis e durante o horário de pregão da B3 (10h às 18h), otimizando o consumo computacional e a cota da API.
* Rate Limiting e Chunking: Processa os ativos em pequenos lotes (fatiamento) com pausas programadas para evitar sobrecarga no servidor de origem e bloqueios por excesso de requisições.
* Armazenamento Eficiente: Os dados são padronizados para o formato longo (Tidy Data) e salvos com compressão Snappy no formato Parquet, otimizados para leitura analítica.

### 2. Dashboard (Visualização)
Interface web construída com Streamlit e Plotly.
* Pattern Matching (Reconhecimento de Padrões): Classifica automaticamente os ativos (Ações, FIIs, ETFs, BDRs) com base no sufixo do código B3 (ticker), permitindo filtros dinâmicos sem onerar o banco de dados.
* Análise Técnica: Filtros interativos por período e cálculo dinâmico da Média Móvel Simples de 20 períodos (SMA 20) com base no histórico de fechamentos diários.

## Tecnologias Utilizadas
* Linguagem: Python (Pandas, Requests)
* Interface: Streamlit e Plotly
* Infraestrutura: Docker e Docker Compose
* Armazenamento: Parquet (Particionado)

## Instalação e Execução

A aplicação foi totalmente conteinerizada para garantir que funcione de forma idêntica em qualquer ambiente de desenvolvimento ou produção.

1. Clone o repositório
git clone [https://github.com/HeitorRangel/financial-data-analysis.git](https://github.com/HeitorRangel/financial-data-analysis.git)
cd financial-data-analysis

2. Configure as Variáveis de Ambiente
Crie o arquivo de configuração baseado no modelo fornecido.
cp .env.example .env

Nota: Abra o arquivo .env gerado e insira o seu BRAPI_TOKEN e a lista de ativos desejada.

3. Inicie a Infraestrutura (Docker)
Execute o orquestrador para construir as imagens e iniciar os serviços de forma isolada e em segundo plano (detached mode).
docker-compose up -d --build

5. Acesse o Dashboard
Abra o seu navegador web e acesse: http://localhost:8501 (ou substitua "localhost" pelo IP público do seu servidor em nuvem).

6. Encerrando os Serviços
Para parar a aplicação com segurança, execute:
docker-compose down

Estrutura do Data Lake
Os arquivos Parquet são gerados pelo contêiner do Ingestor e consumidos pelo contêiner do Dashboard através de um volume compartilhado (Shared Volume). Os dados são persistidos fisicamente na seguinte estrutura particionada:
datalake/ano=YYYY/mes=MM/dia=DD/market_data_HHMMSS.parquet
