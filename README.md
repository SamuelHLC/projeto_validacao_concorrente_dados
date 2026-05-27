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
8. [Gráficos Gerados](#8-gráficos-gerados)
9. [Métricas de Desempenho](#9-métricas-de-desempenho)
10. [Saída Esperada](#10-saída-esperada)

---

## 1. Sobre o Tema

Este projeto aplica técnicas de **programação concorrente** ao problema de análise de grandes volumes de dados reais. O desafio central é processar milhões de registros de corridas de táxi de Nova York de forma eficiente, comparando o desempenho de uma abordagem **sequencial** com uma abordagem **paralela**.

O problema se enquadra na categoria de **paralelismo de dados**: o mesmo conjunto de operações (soma, comparação, contagem) é aplicado sobre partições independentes do dataset, tornando-o um candidato ideal ao padrão **Map-Reduce**.

### Por que esse problema é relevante?

O arquivo `yellow_tripdata_2015-01.csv` contém mais de **12 milhões de registros** e ocupa aproximadamente **2 GB** em disco. Processar esse volume de dados de forma sequencial é viável, mas lento. A versão paralela divide o trabalho entre múltiplos processos do sistema operacional, reduzindo o tempo de processamento de forma proporcional ao número de núcleos disponíveis — até o limite imposto pelo overhead de comunicação (Lei de Amdahl).

---

## 2. Dataset

| Campo | Detalhe |
|---|---|
| **Fonte** | NYC Taxi & Limousine Commission (TLC) |
| **Arquivo** | `yellow_tripdata_2015-01.csv` |
| **Período** | Janeiro de 2015 |
| **Registros** | ~12,7 milhões de corridas |
| **Tamanho** | ~2 GB |
| **Licença** | U.S. Government Works |

### Campos utilizados

| Campo | Descrição |
|---|---|
| `trip_distance` | Distância percorrida na corrida (em milhas), reportada pelo taxímetro |

O foco é exclusivamente na coluna `trip_distance`. Registros com valor zero ou inválido são descartados antes do processamento.

---

## 3. Métricas Calculadas

### Métricas principais (requisito do projeto)

| Métrica | Descrição |
|---|---|
| **Soma total** | Soma de todas as distâncias válidas (milhas) |
| **Média** | Distância média por corrida |
| **Maior corrida** | Distância máxima registrada |
| **Menor corrida** | Distância mínima válida (> 0 mi) |

### Métricas adicionais

| Métrica | Descrição |
|---|---|
| **Desvio padrão** | Dispersão das distâncias em torno da média |
| **Mediana (P50)** | Valor central da distribuição |
| **P25 / P75** | Primeiro e terceiro quartis |
| **P90 / P99** | Percentis superiores (corridas longas) |
| **Distribuição por faixas** | Contagem por categoria: curta, média, longa, muito longa, extrema |

---

## 4. Estratégia de Paralelismo

O projeto implementa o padrão **Map-Reduce** usando `multiprocessing.Pool` do Python:

```
┌──────────────────────────────────────────────────────┐
│              PROCESSO PRINCIPAL                      │
│                                                      │
│  1. Lê o CSV inteiro (sequencial, uma única vez)     │
│  2. Divide as linhas em N chunks iguais              │
└──────────────────────┬───────────────────────────────┘
                       │ distribui chunks
         ┌─────────────┼──────────────┐
         ▼             ▼              ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐
   │Processo 1│  │Processo 2│  │Processo N│
   │          │  │          │  │          │
   │  MAP:    │  │  MAP:    │  │  MAP:    │
   │ soma     │  │ soma     │  │ soma     │
   │ contagem │  │ contagem │  │ contagem │
   │ maior    │  │ maior    │  │ maior    │
   │ menor    │  │ menor    │  │ menor    │
   └────┬─────┘  └────┬─────┘  └────┬─────┘
        └─────────────┴──────────────┘
                       │ resultados parciais
         ┌─────────────▼───────────────────┐
         │         REDUCE                  │
         │  combina parciais → resultado   │
         │  final com todas as métricas    │
         └─────────────────────────────────┘
```

### Fase MAP
Cada processo filho recebe seu chunk e computa de forma independente: soma parcial, contagem, máximo, mínimo e lista de distâncias do pedaço.

### Fase REDUCE
O processo principal combina todos os resultados parciais: soma as somas, encontra o máximo global e o mínimo global, e reúne todas as distâncias para calcular percentis e distribuição.

### Por que `multiprocessing` e não `threading`?
Em Python, o **GIL (Global Interpreter Lock)** impede que threads executem código Python em paralelo real. Para tarefas CPU-bound como esta, `multiprocessing` cria processos separados com memória independente, contornando o GIL e obtendo paralelismo verdadeiro.

---

## 5. Estrutura do Projeto

```
projeto/
│
├── yellow_tripdata_2015-01.csv   ← dataset (baixar do Kaggle)
│
├── taxi_sequential.py            ← versão sequencial (1 processo)
├── taxi_parallel.py              ← versão paralela (N processos)
├── taxi_benchmark.py             ← benchmark + geração de gráficos
│
├── sequential_results.json       ← gerado pelo sequencial
│
├── resultados_paralelos/         ← gerado pelo paralelo
│   └── resultado_p04.json
│
└── graficos_benchmark/           ← gerado pelo benchmark
    ├── grafico_tempo.png
    ├── grafico_speedup.png
    ├── grafico_eficiencia.png
    ├── grafico_barras_tempo.png
    ├── grafico_distribuicao.png
    ├── grafico_estatisticas.png
    └── benchmark_dados.json
```

---

## 6. Pré-requisitos e Instalação

### Requisitos

- Python **3.10+**
- Biblioteca `matplotlib` (única dependência externa)

### Instalação

```bash
pip install matplotlib
```

Bibliotecas já inclusas no Python padrão (não precisam ser instaladas):
`csv`, `multiprocessing`, `json`, `time`, `math`, `os`, `sys`, `argparse`

---

## 7. Como Executar

### Passo 1 — Baixe o dataset

Acesse https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data,  
faça download de `yellow_tripdata_2015-01.csv` e coloque-o na mesma pasta dos scripts.

---

### Passo 2 — Versão Sequencial

Processa todo o arquivo com 1 processo. Serve como linha de base para o cálculo de speedup.

```bash
python taxi_sequential.py
```

**Saída:** `sequential_results.json`

---

### Passo 3 — Versão Paralela

Processa o arquivo dividindo o trabalho entre N processos.

```bash
# Usa todos os núcleos disponíveis na máquina
python taxi_parallel.py

# Define manualmente o número de processos
python taxi_parallel.py --processos 4
python taxi_parallel.py -p 8
```

**Saída:** `resultados_paralelos/resultado_p04.json`

---

### Passo 4 — Benchmark completo + Gráficos

Executa automaticamente com 1, 2, 4 e 8 processos (média de 3 rodadas cada) e gera todos os gráficos.

```bash
# Configuração padrão (até 8 processos ou o máximo da máquina)
python taxi_benchmark.py

# Define o número máximo de processos a testar
python taxi_benchmark.py --max-processos 8
python taxi_benchmark.py -m 16
```

**Saída:** pasta `graficos_benchmark/` com gráficos PNG e `benchmark_dados.json`

---

## 8. Gráficos Gerados

| Arquivo | Descrição |
|---|---|
| `grafico_tempo.png` | Linha do tempo de execução por número de processos |
| `grafico_speedup.png` | Speedup real vs. speedup ideal (linha diagonal) |
| `grafico_eficiencia.png` | Eficiência em % por número de processos |
| `grafico_barras_tempo.png` | Comparativo visual de tempo em barras |
| `grafico_distribuicao.png` | Pizza + barras da distribuição por faixa de distância |
| `grafico_estatisticas.png` | Box-plot sintético com percentis das distâncias |

---

## 9. Métricas de Desempenho

### Speedup
Mede o ganho de velocidade ao usar P processos em relação à execução sequencial:

```
Speedup(P) = T(1) / T(P)
```

- `T(1)` = tempo com 1 processo (sequencial)
- `T(P)` = tempo com P processos
- Speedup ideal = P (proporcional ao número de processos)
- Speedup real < ideal devido ao overhead de comunicação e à parcela sequencial (Lei de Amdahl)

### Eficiência
Mede o aproveitamento de cada processo em relação ao ideal:

```
Eficiência(P) = Speedup(P) / P × 100%
```

- 100% = todos os processos trabalhando o tempo todo (ideal)
- Na prática cai conforme P aumenta, pois o overhead cresce

### Lei de Amdahl
O speedup máximo teórico é limitado pela fração sequencial do programa (leitura do CSV, fase reduce):

```
Speedup_máximo = 1 / (S + (1 - S) / P)
```

Onde `S` é a fração do tempo que não pode ser paralelizada.

---

## 10. Saída Esperada

### Versão Sequencial

```
============================================================
  NYC Yellow Taxi  —  Processamento SEQUENCIAL
============================================================

  RESUMO DAS CORRIDAS
  Total de corridas válidas     :     12,748,986

  ──────────────────────────────────────────────────────
  DISTÂNCIAS (milhas)
  ──────────────────────────────────────────────────────
  Soma total                    : 30,245,817.6543
  Média                         :          2.3724
  Maior corrida                 :        810.0000
  Menor corrida                 :          0.0100
  Desvio padrão                 :          2.8901

  ──────────────────────────────────────────────────────
  PERCENTIS
  ──────────────────────────────────────────────────────
  P25 (1º quartil)              :          1.0000
  P50 (mediana)                 :          1.7000
  P75 (3º quartil)              :          3.1000
  P90                           :          5.3000
  P99                           :         13.4000

  ──────────────────────────────────────────────────────
  DISTRIBUIÇÃO POR FAIXA
  ──────────────────────────────────────────────────────
  Curta      (0 – 1 mi):   3,842,102  (30.1%)  ███████████████
  Média      (1 – 3 mi):   5,629,488  (44.2%)  ██████████████████████
  Longa      (3 – 7 mi):   2,437,901  (19.1%)  █████████
  Muito longa(7 – 15 mi):    694,312  ( 5.4%)  ██
  Extrema    (> 15 mi):     145,183  ( 1.1%)  

  ──────────────────────────────────────────────────────
  DESEMPENHO
  ──────────────────────────────────────────────────────
  Tempo de execução             :        142.3817 s
============================================================
```

### Benchmark

```
  Processos     Tempo (s)      Speedup    Eficiência
  ──────────────────────────────────────────────────
          1      142.3817        1.000x       100.0%
          2       78.4201        1.815x        90.7%
          4       43.9115        3.243x        81.1%
          8       26.7803        5.317x        66.5%
```
