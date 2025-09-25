# Projeto_Grafo_TCC

Análise e visualização de ocorrências (REDS) em grafo, usando **Neo4j** como banco de grafos, **Python** para ETL e carga de dados, e **Neovis.js** para visualização interativa em canvas.

---

## 📂 Estrutura do projeto

```
Projeto_Grafo_TCC/
├─ docker/                  # arquivos de infraestrutura Docker
│  ├─ docker-compose.yml
│  └─ .env.example          # exemplo de variáveis de ambiente
├─ neo4j/                   # volumes persistentes (não versionados)
├─ data/                    # planilhas de entrada (.xlsx) e dados de import
├─ exports/                 # saída de exportações (graphml, csv)
├─ src/                     # scripts Python
│  ├─ load_to_neo4j.py      # carrega os dados do Excel para o Neo4j
│  ├─ popular_dimensoes.py  # povoa dimensões auxiliares
│  ├─ popular_dimensoes_v2.py
│  └─ app_streamlit.py      # interface exploratória em Streamlit
├─ web/                     # frontend Neovis.js
│  ├─ index.html
│  ├─ config.sample.js      # modelo de config (sem credenciais)
│  └─ Dockerfile            # Nginx para servir o Neovis
├─ requirements.txt         # dependências Python
└─ README.md                # este documento
```

---

## 🚀 Como rodar localmente

### 1. Pré-requisitos
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Python 3.10+](https://www.python.org/downloads/) (com `venv`)
- Git

### 2. Clone o repositório
```bash
git clone https://github.com/<seu-usuario>/Projeto_Grafo_TCC.git
cd Projeto_Grafo_TCC
```

### 3. Configure variáveis de ambiente
Crie o arquivo `docker/.env` baseado no `.env.example`:
```env
NEO4J_AUTH=neo4j/senha-forte
```

### 4. Suba os serviços
```bash
cd docker
docker compose up -d
```

Isso vai iniciar:
- **Neo4j** → `http://localhost:7474`
- **Neovis App** → `http://localhost:8080`
- (opcional) **NeoDash** → `http://localhost:5005`

### 5. Ative o ambiente Python
```bash
python -m venv venvTCC
venvTCC/Scripts/activate   # (Windows)
pip install -r requirements.txt
```

### 6. Carregue os dados
Coloque sua planilha base (por ex. `modelo_grafo_REDS_v2.xlsx`) em `data/`.

Execute:
```bash
python src/load_to_neo4j.py
```

Isso vai:
- Ler os dados de `data/modelo_grafo_REDS_v2.xlsx`
- Popular tabelas de dimensão
- Criar nós e relacionamentos no Neo4j

---

## 🎨 Visualização

### Neovis (canvas interativo)
Acesse: [http://localhost:8080](http://localhost:8080)

- Dropdown → escolher recorte (ex.: Ocorrências x Natureza)
- **Atualizar** → roda nova query
- **Exportar PNG** → salva imagem do canvas

### Neo4j Browser
Acesse: [http://localhost:7474](http://localhost:7474)

Queries úteis:
```cypher
MATCH (n) RETURN labels(n)[0] AS label, count(*) ORDER BY count(*) DESC;
MATCH ()-[r]->() RETURN type(r), count(*) ORDER BY count(*) DESC;
```

### NeoDash (opcional)
Dashboards prontos em [http://localhost:5005](http://localhost:5005)

---

## 🔐 Segurança (para VPS)
- Crie um usuário somente-leitura para o Neovis:
```cypher
CREATE USER ro_user SET PASSWORD 'ro_senha' CHANGE NOT REQUIRED;
GRANT ACCESS ON HOME DATABASE TO ro_user;
GRANT MATCH {*} ON GRAPH neo4j NODES * TO ro_user;
GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS * TO ro_user;
DENY WRITE ON GRAPH neo4j TO ro_user;
```
- No `index.html`, use `ro_user/ro_senha` em vez de `neo4j/senha-forte`.

---

## 📊 Próximos passos
- Adicionar algoritmos de **Graph Data Science** (ex.: Louvain, PageRank)
- Integrar análises estatísticas via **Streamlit**
- Automatizar ETL (cron jobs / Airflow / Docker service)

---

## 📜 Licença
Projeto acadêmico para TCC (Pós-Graduação em Ciência de Dados e Big Data – PMMG & IFSULDEMINAS).  
Uso livre para fins acadêmicos. Para outros usos, consulte o autor.
