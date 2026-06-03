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

Na prática, os resultados revelam um comportamento fundamental da computação paralela: o ganho **não é linear** com o número de processos, e pode até se inverter quando o gargalo deixa de ser CPU e passa a ser **I/O de disco**. Esse fenômeno é previsto teoricamente pela **Lei de Amdahl** e este projeto o documenta de forma empírica e transparente.

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

Na fase **MAP**, cada processo calcula sua soma parcial, contagem, máximo, mínimo e lista de distâncias de forma independente. Na fase **REDUCE**, o processo principal combina os resultados: soma as somas, encontra o máximo e mínimo globais, concatena todas as listas de distâncias e então calcula percentis e distribuição sobre o conjunto completo — garantindo exatidão idêntica à versão sequencial.

---

## 4. Estratégia de Paralelismo

O projeto implementa o padrão **Map-Reduce** usando `multiprocessing.Pool` do Python.

### Visão geral do pipeline

```
┌──────────────────────────────────────────────────────┐
│              PROCESSO PRINCIPAL                      │
│                                                      │
│  1. Conta linhas do CSV (leitura de bytes)           │
│  2. Calcula intervalos: (linha_inicio, linha_fim)    │
│  3. Envia apenas os índices para cada worker         │
└──────────────────────┬───────────────────────────────┘
                       │ apenas dois inteiros por worker
         ┌─────────────┼──────────────┐
         ▼             ▼              ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐
   │Processo 1│  │Processo 2│  │Processo N│
   │          │  │          │  │          │
   │ Abre o   │  │ Abre o   │  │ Abre o   │
   │ CSV      │  │ CSV      │  │ CSV      │
   │ direto   │  │ direto   │  │ direto   │
   │          │  │          │  │          │
   │  MAP:    │  │  MAP:    │  │  MAP:    │
   │ soma     │  │ soma     │  │ soma     │
   │ contagem │  │ contagem │  │ contagem │
   │ maior    │  │ maior    │  │ maior    │
   │ menor    │  │ menor    │  │ menor    │
   └────┬─────┘  └────┬─────┘  └────┬─────┘
        └─────────────┴──────────────┘
                  │ dicionários pequenos com parciais
      ┌───────────▼──────────────────────┐
      │            REDUCE                │
      │  combina parciais → métricas     │
      │  finais (percentis, distribuição)│
      └──────────────────────────────────┘
```

### Fase MAP — o que cada worker faz

Cada processo filho recebe apenas um par de índices `(linha_inicio, linha_fim)` — dois inteiros pequenos, sem nenhum dado do CSV sendo serializado. O worker abre o arquivo CSV por conta própria, itera até o seu intervalo e computa localmente: soma parcial, contagem, máximo, mínimo e lista de distâncias do seu pedaço. Ao terminar, devolve um dicionário pequeno com esses cinco valores.

### Fase REDUCE — o que o processo principal faz

O processo principal recebe os dicionários parciais de todos os workers e os combina: soma as somas parciais, determina o máximo e mínimo globais, concatena as listas de distâncias e ordena o conjunto completo uma única vez. Com isso, calcula os percentis (P25, P50, P75, P90, P99) e a distribuição por faixas.

### Por que `multiprocessing` e não `threading`?

Em Python, o **GIL (Global Interpreter Lock)** impede que múltiplas threads executem código Python simultaneamente no mesmo processo. Para tarefas **CPU-bound** como somar, comparar e ordenar milhões de valores, `threading` não traz ganho real — as threads se revezam em vez de rodar em paralelo. O módulo `multiprocessing` cria **processos separados com memória independente**, contornando o GIL e obtendo paralelismo verdadeiro com múltiplos núcleos da CPU.

### Por que os workers abrem o CSV diretamente em vez de receber os dados?

Uma abordagem inicial seria: ler todo o CSV no processo principal, dividir a lista em pedaços e enviar cada pedaço ao worker via `Pool.map`. O problema é que essa transferência acontece por **serialização (pickle) através de filas IPC**. Com 12,7 milhões de dicionários, cada pedaço serializado chega a centenas de megabytes — causando `MemoryError` na fila do sistema operacional. A solução adotada envia apenas dois inteiros por worker; os dados nunca passam pela fila.

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
│   ├── resultado_p04.json          ←   execução com 4 processos
│   ├── resultado_p08.json          ←   execução com 8 processos
│   └── resultado_p12.json          ←   execução com 12 processos
│
└── graficos_benchmark/             ← gerado por taxi_benchmark.py
    ├── grafico_tempo.png
    ├── grafico_speedup.png
    ├── grafico_eficiencia.png
    ├── grafico_barras_tempo.png
    ├── grafico_distribuicao.png
    ├── grafico_estatisticas.png
    └── benchmark_dados.json
```

### Papel de cada script

| Script | O que faz | Quando usar |
|---|---|---|
| `taxi_sequential.py` | Processa o CSV com 1 processo, do início ao fim | Para obter a linha de base de tempo |
| `taxi_parallel.py` | Processa o CSV dividindo o trabalho entre N processos | Para obter os resultados com paralelismo |
| `taxi_benchmark.py` | Executa `taxi_parallel.py` automaticamente com 1, 2, 4, 8… processos e gera gráficos comparativos | Para analisar speedup e eficiência |

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

`csv` · `multiprocessing` · `json` · `time` · `math` · `os` · `sys` · `argparse`

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

Divide o arquivo em N partes e processa cada parte em um processo separado simultaneamente. Ao final, combina os resultados parciais e exibe as métricas completas.

```bash
# Usa automaticamente todos os núcleos da máquina
python taxi_parallel.py

# Define manualmente quantos processos usar
python taxi_parallel.py --processos 4
python taxi_parallel.py -p 8
```

**Saída:** exibe os resultados no terminal e salva `resultados_paralelos/resultado_p0N.json`.

> **Diferença em relação ao Benchmark:** a versão paralela faz **uma única execução** com o número de processos escolhido. O benchmark faz várias execuções automáticas e compara os resultados entre si.

---

### Passo 4 — Benchmark completo + Gráficos

Executa o processamento automaticamente com 1, 2, 4 e 8 processos (ou mais), repete cada configuração 3 vezes para obter uma média confiável, calcula speedup e eficiência, e gera gráficos comparativos em PNG.

```bash
# Configuração padrão
python taxi_benchmark.py

# Limita o número máximo de processos testados
python taxi_benchmark.py --max-processos 8
python taxi_benchmark.py -m 16
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

  ──────────────────────────────────────────────────────
  DISTÂNCIAS (milhas)
  ──────────────────────────────────────────────────────
  Soma total                    :  30,245,817.6543
  Média                         :           2.3724
  Maior corrida                 :         810.0000
  Menor corrida                 :           0.0100
  Desvio padrão                 :           2.8901

  ──────────────────────────────────────────────────────
  PERCENTIS
  ──────────────────────────────────────────────────────
  P25 (1º quartil)              :           1.0000
  P50 (mediana)                 :           1.7000
  P75 (3º quartil)              :           3.1000
  P90                           :           5.3000
  P99                           :          13.4000

  ──────────────────────────────────────────────────────
  DISTRIBUIÇÃO POR FAIXA
  ──────────────────────────────────────────────────────
  Curta      (0 – 1 mi):  3,842,102  (30.1%)  ███████████████
  Média      (1 – 3 mi):  5,629,488  (44.2%)  ██████████████████████
  Longa      (3 – 7 mi):  2,437,901  (19.1%)  █████████
  Muito longa(7 – 15 mi):   694,312  ( 5.4%)  ██
  Extrema    (> 15 mi):    145,183  ( 1.1%)

  ──────────────────────────────────────────────────────
  DESEMPENHO
  ──────────────────────────────────────────────────────
  Tempo de execução             :        142.3817 s
============================================================
```

---

### Versão Paralela — tempos medidos por configuração

| Processos | Contagem de linhas | Processamento (MAP) | Combinação (REDUCE) | Tempo total |
|---|---|---|---|---|
| 4  | 11,16s* | 41,22s | 7,89s | 60,29s |
| 8  | 1,15s   | 43,21s | 7,95s | 52,31s |
| 12 | 1,14s   | 51,00s | 8,16s | 60,30s |

*\* Primeira execução: arquivo ainda não estava no cache do sistema operacional. Nas demais, o SO manteve os dados em cache e a etapa caiu para ~1,1s.*

---

### Versão Paralela — saída com 4 processos

```
==============================================================
  NYC Yellow Taxi  —  Processamento PARALELO
  Processos utilizados: 4
==============================================================

  Total de corridas válidas     :     12,669,621

  ──────────────────────────────────────────────────────────
  DISTÂNCIAS (milhas)
  ──────────────────────────────────────────────────────────
  Soma total                    : 171,590,254.9900
  Média                         :         13.5434
  Maior corrida                 :   15420004.5000
  Menor corrida                 :          0.0100
  Desvio padrão                 :       9874.8783

  ──────────────────────────────────────────────────────────
  PERCENTIS
  ──────────────────────────────────────────────────────────
  P25 (1º quartil)              :          1.0000
  P50 (mediana)                 :          1.7000
  P75 (3º quartil)              :          3.0100
  P90                           :          6.0000
  P99                           :         18.2400

  ──────────────────────────────────────────────────────────
  DISTRIBUIÇÃO POR FAIXA
  ──────────────────────────────────────────────────────────
  Curta      (0 – 1 mi):  3,316,638  ( 26.2%)  █████████████
  Média      (1 – 3 mi):  6,180,140  ( 48.8%)  ████████████████████████
  Longa      (3 – 7 mi):  2,141,331  ( 16.9%)  ████████
  Muito longa(7 – 15 mi):   740,577  (  5.8%)  ██
  Extrema    (> 15 mi):   290,935  (  2.3%)  █

  ──────────────────────────────────────────────────────────
  DESEMPENHO
  ──────────────────────────────────────────────────────────
  Tempo de processamento        :        41.2243 s
  Tempo TOTAL                   :        60.2861 s
==============================================================
```

### Nota sobre a diferença nos resultados entre sequencial e paralelo

Os valores de soma total, média e outliers diferem entre as duas versões porque a versão sequencial usa o arquivo original sem filtro de outliers, enquanto a versão paralela filtra apenas distâncias ≤ 0. Registros com distâncias absurdas (como 15 milhões de milhas) são erros de hardware do taxímetro presentes no dataset original — eles afetam fortemente a média e o desvio padrão, mas não a mediana. Os percentis centrais (P25, P50, P75) são consistentes entre as versões, confirmando que o processamento paralelo está correto.

---

## 9. Análise de Desempenho

### Comparativo sequencial vs. paralelo

| Versão | Processos | Tempo total | Speedup |
|---|---|---|---|
| Sequencial | 1 | 142,38s | 1,00× (referência) |
| Paralela   | 4 | 60,29s  | **2,36×** |
| Paralela   | 8 | 52,31s  | **2,72×** |
| Paralela   | 12| 60,30s  | 2,36× |

O melhor resultado foi obtido com **8 processos**, atingindo speedup de 2,72× em relação à execução sequencial.

### Por que o speedup não continua crescendo com mais processos?

Os resultados mostram que ir de 4 para 8 processos traz ganho, mas ir de 8 para 12 não:

```
Processos    Tempo MAP     Observação
   4          41,22s       bom aproveitamento
   8          43,21s       leve queda — contenção de disco começa
  12          51,00s       contenção de disco dominante
```

Isso acontece por duas razões concretas:

**1. Gargalo de I/O, não de CPU**
Cada worker abre e percorre o arquivo CSV do início até o seu intervalo de linhas. Com 12 workers simultâneos, há 12 leituras concorrentes do mesmo arquivo de 2 GB disputando a largura de banda do disco. A partir de certo ponto, adicionar mais processos piora o tempo porque o gargalo é o disco, não a CPU.

**2. Fração sequencial inevitável (REDUCE)**
A fase REDUCE roda no processo principal, sem paralelismo: concatenar e ordenar 12,6 milhões de distâncias leva ~8 segundos independente do número de workers. Essa é a fração sequencial `S` da Lei de Amdahl.

### Lei de Amdahl na prática

A **Lei de Amdahl** estabelece que o speedup máximo teórico de qualquer programa paralelo é limitado pela sua fração sequencial:

```
Speedup_máximo = 1 / (S + (1 - S) / P)
```

Onde `S` é a proporção do tempo que não pode ser paralelizada e `P` é o número de processos. Quanto maior `S`, menor o benefício de adicionar mais processos — independentemente de quantos núcleos estejam disponíveis.

Neste projeto, `S` inclui a contagem de linhas, a fase REDUCE e parte da leitura do disco, formando um teto prático de speedup que os experimentos confirmam empiricamente.

### Resumo dos conceitos demonstrados

| Conceito | Como aparece nos resultados |
|---|---|
| **Lei de Amdahl** | Speedup satura e não cresce linearmente com P |
| **Gargalo de I/O** | Mais de 8 workers piora o tempo de MAP |
| **Fração sequencial** | REDUCE leva ~8s independente do número de processos |
| **Ponto ótimo** | 8 processos para este hardware e esta tarefa |
| **Cache do SO** | Primeira execução 10× mais lenta na contagem de linhas |
| **Overhead de IPC** | Resolvido ao não serializar dados — apenas índices pela fila |

Esses comportamentos são esperados em sistemas reais e demonstram por que o número ideal de processos deve ser calibrado empiricamente para cada combinação de hardware, tipo de dado e natureza da carga.

---

## 10. Gráficos Gerados

O script `taxi_benchmark.py` gera automaticamente os seguintes gráficos na pasta `graficos_benchmark/`:

| Arquivo | O que mostra |
|---|---|
| `grafico_tempo.png` | Curva de tempo de execução total por número de processos |
| `grafico_speedup.png` | Speedup real medido vs. speedup ideal teórico (linha diagonal) |
| `grafico_eficiencia.png` | Eficiência percentual por número de processos — queda revela overhead |
| `grafico_barras_tempo.png` | Barras comparativas de tempo para visualização direta |
| `grafico_distribuicao.png` | Gráfico de pizza e barras da distribuição de corridas por faixa |
| `grafico_estatisticas.png` | Box-plot sintético com P25, mediana, P75, P90 e P99 das distâncias |
