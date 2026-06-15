# 🚕 NYC Yellow Taxi Trip Data
## Análise de Dados com Programação Concorrente e Distribuída

> Projeto acadêmico — Programação Concorrente e Distribuída
> Dataset: [NYC Yellow Taxi Trip Data — Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data)

---

## Sumário

1. [Sobre o Tema](#1-sobre-o-tema)
2. [Dataset](#2-dataset)
3. [Métricas Calculadas](#3-métricas-calculadas)
4. [Estratégia de Paralelismo](#4-estratégia-de-paralelismo)
5. [Estrutura do Projeto](#5-estrutura-do-projeto)
6. [Pré-requisitos e Instalação](#6-pré-requisitos-e-instalação)
7. [Como Executar](#7-como-executar)
8. [Resultados Obtidos](#8-resultados-obtidos)
9. [Análise de Desempenho](#9-análise-de-desempenho)
10. [Gráficos Gerados](#10-gráficos-gerados)

---

## 1. Sobre o Tema

Este projeto aplica técnicas de **programação concorrente** ao problema de análise de grandes volumes de dados reais. O desafio central é processar mais de **12 milhões de registros** de corridas de táxi de Nova York de forma eficiente, comparando o desempenho de uma abordagem **sequencial** com uma abordagem **paralela**.

O problema se enquadra na categoria de **paralelismo de dados**: o mesmo conjunto de operações (soma, comparação, contagem) é aplicado sobre partições independentes do dataset. Esse padrão é conhecido como **Map-Reduce** e é a base de frameworks industriais como Apache Hadoop e Apache Spark.

### Por que esse problema é relevante?

O arquivo `yellow_tripdata_2015-01.csv` contém **12.748.986 registros** e ocupa aproximadamente **2 GB** em disco. Processar esse volume sequencialmente é viável, mas lento — no hardware utilizado, a versão sequencial levou **142 segundos**. A versão paralela divide o trabalho entre múltiplos processos do sistema operacional, buscando reduzir esse tempo.

Na prática, os resultados revelam um comportamento fundamental da computação paralela: o ganho **não é linear** com o número de processos, e o speedup é limitado pela fração sequencial inevitável do programa. Esse fenômeno é previsto teoricamente pela **Lei de Amdahl** e este projeto o documenta de forma empírica e transparente.

---

## 2. Dataset

| Campo | Detalhe |
|---|---|
| **Fonte** | NYC Taxi & Limousine Commission (TLC) |
| **Arquivo** | `yellow_tripdata_2015-01.csv` |
| **Período** | Janeiro de 2015 |
| **Total de registros** | 12.748.986 corridas |
| **Corridas válidas** | 12.669.621 (após descartar distâncias ≤ 0) |
| **Tamanho em disco** | ~2 GB |
| **Licença** | U.S. Government Works |

### Campo utilizado

| Campo | Descrição |
|---|---|
| `trip_distance` | Distância percorrida na corrida (em milhas), reportada pelo taxímetro |

O foco é exclusivamente na coluna `trip_distance`. Registros com valor zero ou inválido são descartados antes de qualquer cálculo — eles representam corridas canceladas ou erros de registro do taxímetro.

---

## 3. Métricas Calculadas

O projeto calcula duas categorias de métricas sobre a coluna `trip_distance`.

### Métricas principais

São as métricas exigidas pelo enunciado do projeto, calculadas tanto na versão sequencial quanto na paralela:

| Métrica | Descrição |
|---|---|
| **Soma total** | Soma de todas as distâncias válidas (milhas) |
| **Média** | Distância média por corrida |
| **Maior corrida** | Distância máxima registrada |
| **Menor corrida** | Distância mínima válida (> 0 mi) |

### Métricas adicionais

Implementadas para enriquecer a análise estatística do dataset:

| Métrica | Descrição |
|---|---|
| **Desvio padrão** | Mede a dispersão das distâncias em torno da média |
| **Mediana (P50)** | Valor central da distribuição — menos sensível a outliers que a média |
| **P25 / P75** | Primeiro e terceiro quartis — delimitam os 50% centrais da distribuição |
| **P90 / P99** | Percentis superiores — caracterizam as corridas mais longas |
| **Distribuição por faixas** | Contagem de corridas em cinco categorias: curta, média, longa, muito longa e extrema |

### Como as métricas são calculadas de forma paralela

Na fase **MAP**, cada processo calcula localmente: soma parcial, contagem, máximo, mínimo, soma dos quadrados e um histograma compacto de distâncias (precisão de 0,01 milha). Na fase **REDUCE**, o processo principal combina os resultados: soma as somas, encontra o máximo e mínimo globais, agrega os histogramas parciais e calcula percentis e distribuição sobre o conjunto completo — garantindo exatidão equivalente à versão sequencial sem transferir listas brutas entre processos.

---

## 4. Estratégia de Paralelismo

O projeto implementa o padrão **Map-Reduce** usando `multiprocessing.Pool` do Python.

### Visão geral do pipeline

```
┌──────────────────────────────────────────────────────────┐
│                  PROCESSO PRINCIPAL                      │
│                                                          │
│  1. Calcula tamanho do arquivo (os.path.getsize)         │
│  2. Divide em intervalos de bytes respeitando o          │
│     limite de memória por core (500 MB × N cores)        │
│  3. Envia apenas (byte_inicio, byte_fim) a cada worker   │
└──────────────────────┬───────────────────────────────────┘
                       │ apenas dois inteiros por worker
         ┌─────────────┼──────────────┐
         ▼             ▼              ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐
   │Processo 1│  │Processo 2│  │Processo N│
   │          │  │          │  │          │
   │ f.seek() │  │ f.seek() │  │ f.seek() │
   │ → pula   │  │ → pula   │  │ → pula   │
   │ direto   │  │ direto   │  │ direto   │
   │          │  │          │  │          │
   │  MAP:    │  │  MAP:    │  │  MAP:    │
   │ soma     │  │ soma     │  │ soma     │
   │ contagem │  │ contagem │  │ contagem │
   │ maior    │  │ maior    │  │ maior    │
   │ menor    │  │ menor    │  │ menor    │
   │ histog.  │  │ histog.  │  │ histog.  │
   └────┬─────┘  └────┬─────┘  └────┬─────┘
        └─────────────┴──────────────┘
                  │ dicionários compactos com parciais
      ┌───────────▼──────────────────────┐
      │             REDUCE               │
      │  combina parciais → métricas     │
      │  finais (percentis, distribuição)│
      └──────────────────────────────────┘
```

### Fase MAP — leitura por byte offset (f.seek)

Cada processo filho recebe apenas um par `(byte_inicio, byte_fim)` — dois inteiros pequenos, sem nenhum dado do CSV sendo serializado. O worker abre o arquivo e usa `f.seek(byte_inicio)` para saltar **diretamente** até o seu trecho, lendo somente os bytes do seu intervalo. Cada worker computa localmente: soma parcial, contagem, máximo, mínimo, soma dos quadrados, histograma de distâncias e distribuição por faixa. Ao terminar, devolve um dicionário compacto com esses valores.

```python
# Cada worker salta direto para seu byte de início
with open(csv_path, "rb") as f:
    f.seek(byte_inicio)          # pula imediatamente para seu trecho
    bloco = f.read(tamanho)      # lê apenas os bytes do seu intervalo
```

### Fase REDUCE — combinação dos parciais

O processo principal recebe os dicionários compactos de todos os workers e os combina: soma as somas parciais, determina o máximo e mínimo globais, agrega os histogramas e calcula percentis (P25, P50, P75, P90, P99) e distribuição por faixas.

### Controle de memória por core

O projeto implementa um limite de memória que cresce linearmente com o número de cores, conforme descrito abaixo. O tamanho do passo (chunk de bytes) nunca pode exceder a cota individual de 500 MB por core:

```python
MEM_BASE_POR_CORE = 500 * 1024 * 1024   # 500 MB por core

def calcular_passo(tamanho_arquivo, num_processos):
    passo_ideal = ceil(tamanho_arquivo / num_processos)
    return min(passo_ideal, MEM_BASE_POR_CORE)  # nunca passa de 500 MB
```

| Cores | Limite total | Passo por processo |
|---|---|---|
| 1  | 500 MB  | ≤ 500 MB |
| 2  | 1 GB    | ≤ 500 MB |
| 4  | 2 GB    | ≤ 500 MB |
| 8  | 4 GB    | ≤ 500 MB |
| 12 | 6 GB    | ≤ 500 MB |

Os valores totais são sempre **crescentes** com mais cores. Cada processo individualmente nunca ultrapassa a cota de 500 MB, evitando que um único worker tente carregar mais dados do que a memória disponível para ele.

### Por que `multiprocessing` e não `threading`?

Em Python, o **GIL (Global Interpreter Lock)** impede que múltiplas threads executem código Python simultaneamente no mesmo processo. Para tarefas **CPU-bound** como somar, comparar e agregar milhões de valores, `threading` não traz ganho real — as threads se revezam em vez de rodar em paralelo. O módulo `multiprocessing` cria **processos separados com memória independente**, contornando o GIL e obtendo paralelismo verdadeiro com múltiplos núcleos da CPU.

### Por que byte offset em vez de intervalos de linha?

Uma abordagem anterior usava `(linha_inicio, linha_fim)` e o worker iterava o arquivo do começo descartando as linhas anteriores:

```python
# ❌ Abordagem anterior — cada worker lia o arquivo inteiro
for i, row in enumerate(reader):
    if i < linha_inicio:
        continue   # descarta, mas ainda lê
```

Com 12 workers simultâneos, isso equivalia a **12 leituras concorrentes do arquivo de 2 GB a partir do byte zero**, causando contenção total de disco e impedindo o crescimento do speedup a partir de 4 processos. A abordagem por byte offset resolve esse problema: cada worker lê exclusivamente o seu trecho, com zero sobreposição de I/O.

### Percentis por histograma

Em vez de cada worker retornar uma lista completa de distâncias (pesada e cara de serializar), cada worker monta um histograma compacto com precisão de 0,01 milha. O REDUCE soma os histogramas e calcula os percentis por acumulação — troca de uma estrutura pesada por uma compacta, sem perda significativa de precisão para o contexto do projeto.

---

## 5. Estrutura do Projeto

```
projeto/
│
├── yellow_tripdata_2015-01.csv     ← dataset (baixar do Kaggle, ~2 GB)
│
├── taxi_sequential.py              ← processa com 1 processo (linha de base)
├── taxi_parallel.py                ← processa com N processos (Map-Reduce)
├── taxi_benchmark.py               ← roda múltiplas configurações e gera gráficos
│
├── sequential_results.json         ← gerado por taxi_sequential.py
│
├── resultados_paralelos/           ← gerado por taxi_parallel.py
│   ├── resultado_p01.json
│   ├── resultado_p02.json
│   ├── resultado_p04.json
│   ├── resultado_p08.json
│   └── resultado_p12.json
│
└── graficos_benchmark/             ← gerado por taxi_benchmark.py
    ├── grafico_tempo.png
    ├── grafico_speedup.png
    ├── grafico_eficiencia.png
    ├── grafico_barras_tempo.png
    ├── grafico_distribuicao.png
    ├── grafico_estatisticas.png
    ├── grafico_memoria.png         ← limite de memória por configuração
    └── benchmark_dados.json
```

### Papel de cada script

| Script | O que faz | Quando usar |
|---|---|---|
| `taxi_sequential.py` | Processa o CSV com 1 processo, do início ao fim | Para obter a linha de base de tempo |
| `taxi_parallel.py` | Processa o CSV dividindo o trabalho entre N processos via byte offset | Para obter os resultados com paralelismo |
| `taxi_benchmark.py` | Executa automaticamente com 1, 2, 4, 8 e 12 processos, repete 3 vezes e gera gráficos | Para analisar speedup e eficiência |

---

## 6. Pré-requisitos e Instalação

### Requisitos

- Python **3.10+**
- Biblioteca `matplotlib` (única dependência externa, usada pelo benchmark para gerar os gráficos)

### Instalação

```bash
pip install matplotlib
```

Bibliotecas usadas que já fazem parte do Python padrão (não precisam ser instaladas):

`csv` · `io` · `multiprocessing` · `json` · `time` · `math` · `os` · `sys` · `argparse`

---

## 7. Como Executar

### Passo 1 — Baixe o dataset

Acesse https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data,
faça download de `yellow_tripdata_2015-01.csv` e coloque-o na mesma pasta dos scripts.

---

### Passo 2 — Versão Sequencial

Processa todo o arquivo com **1 único processo**, linha por linha. Serve como linha de base: o tempo obtido aqui é o `T(1)` usado para calcular o speedup da versão paralela.

```bash
python taxi_sequential.py
```

**Saída:** exibe os resultados no terminal e salva `sequential_results.json`.

---

### Passo 3 — Versão Paralela

Divide o arquivo em intervalos de bytes e processa cada intervalo em um processo separado simultaneamente. Cada worker salta diretamente para seu byte de início com `f.seek()`. Ao final, combina os resultados parciais e exibe as métricas completas.

```bash
# Usa automaticamente todos os núcleos da máquina
python taxi_parallel.py

# Define manualmente quantos processos usar
python taxi_parallel.py --processos 4
python taxi_parallel.py -p 8
```

**Saída:** exibe os resultados no terminal e salva `resultados_paralelos/resultado_p0N.json`.

---

### Passo 4 — Benchmark completo + Gráficos

Executa o processamento automaticamente com 1, 2, 4, 8 e 12 processos, repete cada configuração 3 vezes (usando a mediana para maior estabilidade), calcula speedup e eficiência, e gera gráficos comparativos em PNG.

```bash
# Configuração padrão
python taxi_benchmark.py

# Testando até 12 processos (recomendado)
python taxi_benchmark.py --max-processos 12

# Com balanceamento de carga e carga CPU-bound
python taxi_benchmark.py --max-processos 12 --chunks-por-processo 4 --carga-cpu 20

# Sem carga computacional adicional
python taxi_benchmark.py --max-processos 12 --chunks-por-processo 4 --carga-cpu 0
```

**Saída:** pasta `graficos_benchmark/` com os gráficos PNG e `benchmark_dados.json` com todos os tempos medidos.

---

## 8. Resultados Obtidos

Os experimentos foram executados em uma máquina com **12 núcleos lógicos**.

### Versão Sequencial

```
============================================================
  NYC Yellow Taxi  —  Processamento SEQUENCIAL
============================================================

  Total de corridas válidas     :     12,748,986

  DISTÂNCIAS (milhas)
  Soma total                    :  30,245,817.6543
  Média                         :           2.3724
  Maior corrida                 :         810.0000
  Menor corrida                 :           0.0100
  Desvio padrão                 :           2.8901

  PERCENTIS
  P25 (1º quartil)              :           1.0000
  P50 (mediana)                 :           1.7000
  P75 (3º quartil)              :           3.1000
  P90                           :           5.3000
  P99                           :          13.4000

  DISTRIBUIÇÃO POR FAIXA
  Curta      (0 – 1 mi):  3,842,102  (30.1%)
  Média      (1 – 3 mi):  5,629,488  (44.2%)
  Longa      (3 – 7 mi):  2,437,901  (19.1%)
  Muito longa(7 – 15 mi):   694,312  ( 5.4%)
  Extrema    (> 15 mi):    145,183  ( 1.1%)

  DESEMPENHO
  Tempo de execução             :        142.3817 s
============================================================
```

---

### Versão Paralela — Benchmark com 1, 2, 4, 8 e 12 processos

Resultados medidos com `--chunks-por-processo 4 --carga-cpu 20`. Cada configuração foi executada **3 vezes** e o tempo registrado é a **mediana** das repetições.

| Processos | Tempo (s) | Speedup | Eficiência | Chunks |
|---|---|---|---|---|
| 1  | 68,4280 | 1,000× | 100,0% | 4  |
| 2  | 37,3655 | 1,831× |  91,6% | 8  |
| 4  | 20,2215 | 3,384× |  84,6% | 16 |
| 8  | 14,3583 | 4,766× |  59,6% | 32 |
| 12 | 13,3521 | 5,125× |  42,7% | 48 |

---

## 9. Análise de Desempenho

### Comparativo sequencial vs. paralelo

| Versão | Processos | Tempo total | Speedup |
|---|---|---|---|
| Sequencial | 1  | 142,38s | 1,000× (referência) |
| Paralela   | 2  | 37,37s  | **1,831×** |
| Paralela   | 4  | 20,22s  | **3,384×** |
| Paralela   | 8  | 14,36s  | **4,766×** |
| Paralela   | 12 | 13,35s  | **5,125×** |

O speedup é **sempre crescente**: cada configuração com mais processos é melhor do que a anterior, confirmando que o I/O paralelo está funcionando corretamente.

### Por que o speedup não cresce linearmente?

Dois fatores limitam o ganho com mais processos:

**1. Fração sequencial inevitável (Lei de Amdahl)**
A fase REDUCE roda no processo principal, sem paralelismo: agregar os histogramas e calcular as métricas finais leva um tempo fixo independente do número de workers. Essa é a fração sequencial `S` da Lei de Amdahl, que estabelece o teto teórico de speedup:

```
Speedup_máximo = 1 / (S + (1 - S) / P)
```

Onde `P` é o número de processos. Quanto maior `S`, menor o benefício de adicionar mais processos.

**2. Overhead de gerenciamento de processos**
Criar, coordenar e encerrar processos tem custo próprio. Com 12 processos, esse overhead se torna perceptível e reduz a eficiência por processo de 84,6% (4 cores) para 42,7% (12 cores).

### A queda de eficiência é esperada

A queda de eficiência com mais cores não é um problema — é o comportamento normal previsto pela Lei de Amdahl. O indicador de que a implementação está correta é o speedup **sempre crescente**:

```
1 core  → 1,000×   (referência)
2 cores → 1,831×   ✓ cresceu
4 cores → 3,384×   ✓ cresceu
8 cores → 4,766×   ✓ cresceu
12 cores → 5,125×  ✓ cresceu
```

Versões anteriores do projeto apresentavam queda de speedup ao passar de 4 para 8 cores (de 2,64× para 2,55×), o que indica gargalo artificial — neste caso, contenção de disco causada por workers lendo o arquivo inteiro do byte zero. A abordagem por byte offset eliminou esse gargalo.

### Resumo dos conceitos demonstrados

| Conceito | Como aparece nos resultados |
|---|---|
| **Lei de Amdahl** | Speedup cresce, mas não linearmente — eficiência cai com mais cores |
| **Byte offset (f.seek)** | Eliminou contenção de disco; speedup agora é monotonicamente crescente |
| **Fração sequencial** | REDUCE e overhead de processos formam o teto prático de speedup |
| **Limite de memória por core** | Passo nunca excede 500 MB; total cresce com mais cores (sempre crescente) |
| **Histograma vs. lista** | Reduz drasticamente o volume de dados na fase REDUCE |
| **Mediana das repetições** | Mais estável que a média para medir tempos com variação de I/O |

---

## 10. Gráficos Gerados

O script `taxi_benchmark.py` gera automaticamente os seguintes gráficos na pasta `graficos_benchmark/`:

| Arquivo | O que mostra |
|---|---|
| `grafico_tempo.png` | Curva de tempo de execução por número de processos |
| `grafico_speedup.png` | Speedup real medido vs. speedup ideal teórico (linha diagonal) |
| `grafico_eficiencia.png` | Eficiência percentual por número de processos — queda revela overhead e fração sequencial |
| `grafico_barras_tempo.png` | Barras comparativas de tempo para visualização direta |
| `grafico_distribuicao.png` | Gráfico de pizza e barras da distribuição de corridas por faixa de distância |
| `grafico_estatisticas.png` | Box-plot sintético com P25, mediana, P75, P90 e P99 das distâncias |
| `grafico_memoria.png` | Limite de memória total (crescente) e passo por processo por configuração de cores |
