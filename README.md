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
6. [Ambiente de Execução](#6-ambiente-de-execução)
7. [Pré-requisitos e Instalação](#7-pré-requisitos-e-instalação)
8. [Como Executar](#8-como-executar)
9. [Speedup e Resultados do Benchmark](#9-speedup-e-resultados-do-benchmark)
10. [Métricas e Distribuição das Corridas](#10-métricas-e-distribuição-das-corridas)
11. [Análise de Desempenho](#11-análise-de-desempenho)
12. [Gráficos Complementares](#12-gráficos-complementares)
13. [Conclusão](#13-conclusão)

---

## 1. Sobre o Tema

Este projeto aplica técnicas de **programação concorrente** ao problema de análise de grandes volumes de dados reais. O objetivo é processar milhões de registros de corridas de táxi de Nova York e comparar o desempenho entre uma abordagem sequencial e uma abordagem paralela baseada em múltiplos processos.

O problema se enquadra em **paralelismo de dados**: a mesma operação é aplicada sobre partes independentes do dataset. Esse padrão se aproxima do modelo **Map-Reduce**, no qual cada processo calcula resultados parciais e o processo principal combina essas parciais para produzir o resultado final.

Neste benchmark, o processamento com **1 processo** levou **282,07 segundos** em média. A melhor configuração testada foi com **12 processos**, levando **103,35 segundos**, com speedup de **2,73×**.

---

## 2. Dataset

| Campo | Detalhe |
|---|---|
| **Fonte** | NYC Taxi & Limousine Commission (TLC) |
| **Arquivo** | `yellow_tripdata_2015-01.csv` |
| **Período** | Janeiro de 2015 |
| **Corridas válidas processadas** | 12.669.621 |
| **Campo analisado** | `trip_distance` |
| **Unidade** | Milhas |

O foco do processamento é a coluna `trip_distance`, que representa a distância percorrida por cada corrida. Registros com distância menor ou igual a zero são descartados para evitar distorções nas métricas.

---

## 3. Métricas Calculadas

O projeto calcula métricas estatísticas sobre as distâncias das corridas:

| Métrica | Descrição |
|---|---|
| **Soma total** | Soma de todas as distâncias válidas |
| **Média** | Distância média por corrida |
| **Maior corrida** | Maior distância encontrada |
| **Menor corrida** | Menor distância válida encontrada |
| **Desvio padrão** | Dispersão das distâncias em relação à média |
| **Mediana** | Valor central da distribuição |
| **P25 / P75** | Primeiro e terceiro quartis |
| **P90 / P99** | Percentis superiores da distribuição |
| **Distribuição por faixas** | Quantidade de corridas por intervalo de distância |

---

## 4. Estratégia de Paralelismo

O projeto utiliza `multiprocessing.Pool` para distribuir o processamento entre vários processos.

A estratégia geral é:

1. O processo principal identifica o número de linhas do CSV.
2. O arquivo é dividido em intervalos de linhas.
3. Cada worker recebe apenas o intervalo que deve processar.
4. Cada worker abre o CSV, percorre sua parte e calcula métricas parciais.
5. O processo principal combina as parciais e gera as métricas finais.

### Por que `multiprocessing`?

Em Python, o uso de múltiplas threads é limitado pelo **GIL (Global Interpreter Lock)** em tarefas intensivas de CPU. Com `multiprocessing`, cada processo possui seu próprio interpretador Python, permitindo paralelismo real em múltiplos núcleos.

### Por que cada processo abre o CSV?

Enviar milhões de linhas para os processos por memória exigiria serialização pesada via `pickle`, o que poderia gerar alto consumo de RAM e lentidão. A solução adotada envia apenas índices de início e fim; cada processo lê diretamente sua parte do arquivo.

---

## 5. Estrutura do Projeto

```text
projeto/
│
├── yellow_tripdata_2015-01.csv       ← dataset, não incluído no repositório
│
├── taxi_sequential.py                ← execução sequencial
├── taxi_parallel.py                  ← execução paralela com N processos
├── taxi_benchmark.py                 ← benchmark com múltiplas configurações
│
├── sequential_results.json           ← resultado sequencial individual
├── resultados_paralelos/
│   └── resultado_p20.json            ← resultado paralelo individual já registrado
│
└── graficos_benchmark/
    ├── benchmark_dados.json          ← fonte principal dos resultados do README
    ├── grafico_tempo.png
    ├── grafico_speedup.png
    ├── grafico_eficiencia.png
    ├── grafico_barras_tempo.png
    ├── grafico_distribuicao.png
    └── grafico_estatisticas.png
```

---

## 6. Ambiente de Execução

Os testes de paralelização foram executados em um computador Dell Vostro 3710. A configuração abaixo foi extraída do relatório `DxDiag.txt`, gerado no próprio ambiente em que o benchmark foi executado.

| Componente | Configuração |
|---|---|
| **Sistema operacional** | Windows 11 Pro 64-bit — Build 26100 |
| **Fabricante / modelo** | Dell Inc. — Vostro 3710 |
| **BIOS** | 1.17.0 — UEFI |
| **Processador** | 12th Gen Intel(R) Core(TM) i7-12700 |
| **Núcleos/threads reportados pelo DxDiag** | 20 CPUs lógicas |
| **Frequência base aproximada** | ~2.1 GHz |
| **Memória RAM instalada** | 16.384 MB RAM, aproximadamente 16 GB |
| **Memória disponível para o sistema** | 16.072 MB RAM |
| **Armazenamento principal** | SSD NVMe Micron 2400A — 512 GB |
| **Espaço total da unidade C:** | 487,4 GB |
| **Espaço livre no momento do relatório** | 258,6 GB |
| **GPU** | Intel(R) UHD Graphics 770 integrada |
| **Memória gráfica compartilhada** | 8.036 MB |
| **DirectX** | DirectX 12 |
| **Monitor / resolução** | Dell SE2222H — 1920 × 1080, 60 Hz |
| **Data do relatório** | 09/06/2026 às 17:41:01 |

### Observações sobre o ambiente

A implementação utiliza `multiprocessing`, portanto o desempenho depende principalmente de CPU, memória RAM e velocidade de leitura do disco. A GPU listada no relatório não foi utilizada para acelerar o processamento, pois o projeto não usa CUDA, OpenCL ou bibliotecas de computação em GPU.

Como o dataset é lido a partir de um arquivo CSV grande, o armazenamento também influencia os resultados. Mesmo com vários processos, parte do tempo pode ser limitada por I/O de disco, leitura repetida do arquivo e combinação dos resultados parciais.

---

## 7. Pré-requisitos e Instalação

### Requisitos

- Python 3.10+
- `matplotlib`

Instalação da dependência externa:

```bash
pip install matplotlib
```

Bibliotecas usadas da própria linguagem:

```text
csv · multiprocessing · json · time · math · os · sys · argparse
```

---

## 8. Como Executar

### 1. Baixar o dataset

Baixe o arquivo `yellow_tripdata_2015-01.csv` no Kaggle e coloque-o na mesma pasta dos scripts.

### 2. Execução sequencial

```bash
python taxi_sequential.py
```

Essa execução gera:

```text
sequential_results.json
```

### 3. Execução paralela individual

```bash
python taxi_parallel.py --processos 4
```

Também é possível usar:

```bash
python taxi_parallel.py -p 8
python taxi_parallel.py --processos 12
python taxi_parallel.py --processos 20
```

Essa execução gera arquivos individuais em:

```text
resultados_paralelos/
```

### 4. Benchmark completo

Para gerar os resultados comparativos usados neste README, execute:

```bash
python taxi_benchmark.py --max-processos 12
```

O benchmark testa automaticamente:

```text
1, 2, 4, 8 e 12 processos
```

Cada configuração foi executada **3 vezes**, e o tempo exibido representa a média dessas execuções.

A saída principal é:

```text
graficos_benchmark/benchmark_dados.json
```

Esse arquivo é a fonte oficial da tabela de desempenho deste README.

---

## 9. Speedup e Resultados do Benchmark

> **Seção principal para avaliação do desempenho:** aqui estão reunidos o speedup, a tabela comparativa e os principais gráficos do benchmark.

Os resultados abaixo foram extraídos de:

```text
graficos_benchmark/benchmark_dados.json
```

O benchmark foi executado com **1, 2, 4, 8 e 12 processos**, com **3 repetições por configuração**. O tempo exibido corresponde à **média das execuções**.

### Resumo direto dos resultados

| Indicador | Resultado |
|---|---:|
| Tempo com 1 processo | 282,07 s |
| Melhor tempo paralelo | 103,35 s |
| Melhor configuração | 12 processos |
| Melhor speedup | 2,73× |
| Redução de tempo vs. 1 processo | 63,36% |
| Eficiência com 12 processos | 22,74% |

Em termos práticos, a paralelização reduziu o tempo médio de **282,07 segundos** para **103,35 segundos**, alcançando speedup de **2,73×** na melhor configuração testada.

### Tabela comparativa de desempenho

| Processos | Tempo médio | Speedup | Eficiência | Redução vs. 1 processo |
|---:|---:|---:|---:|---:|
| 1 | 282,07 s | 1,00× | 100,00% | 0,00% |
| 2 | 145,56 s | 1,94× | 96,89% | 48,40% |
| 4 | 106,79 s | 2,64× | 66,03% | 62,14% |
| 8 | 110,63 s | 2,55× | 31,87% | 60,78% |
| 12 | 103,35 s | 2,73× | 22,74% | 63,36% |

### Gráficos principais do benchmark

#### Tempo médio por quantidade de processos

![Tempo médio por quantidade de processos](grafico_tempo.png)

#### Speedup obtido

![Speedup obtido](grafico_speedup.png)

#### Eficiência paralela

![Eficiência paralela](grafico_eficiencia.png)

#### Comparativo em barras

![Comparativo em barras](grafico_barras_tempo.png)

### Leitura rápida do speedup

O **speedup** mede quantas vezes a execução paralela foi mais rápida em relação à execução com 1 processo:

```text
Speedup = Tempo com 1 processo / Tempo com N processos
```

Para a melhor execução:

```text
Speedup = 282,07 / 103,35
Speedup = 2,73×
```

A melhor configuração medida foi com **12 processos**, atingindo **2,73×** de speedup. Isso significa que, nesse ambiente, o processamento paralelo foi aproximadamente **2,73 vezes mais rápido** que a execução com 1 processo.

### Interpretação breve dos resultados

O ganho de desempenho foi mais forte entre **1 e 4 processos**, quando o tempo caiu de **282,07 s** para **106,79 s**. A partir de **8 processos**, o ganho deixou de crescer de forma linear, indicando possível saturação por leitura de disco, overhead de criação/coordenação dos processos e combinação dos resultados parciais.

Mesmo assim, a execução com **12 processos** apresentou o menor tempo geral, com redução aproximada de **63,36%** em relação à execução com 1 processo.

---

## 10. Métricas e Distribuição das Corridas

As métricas abaixo demonstram que o processamento paralelo preservou os resultados estatísticos do dataset analisado.

### Métricas estatísticas

| Métrica | Valor |
|---|---:|
| Total de corridas válidas | 12.669.621 |
| Soma total das distâncias | 171.590.254,99 mi |
| Média | 13,5434 mi |
| Maior corrida | 15.420.004,50 mi |
| Menor corrida | 0,01 mi |
| Desvio padrão | 9.874,8783 |
| P25 | 1,00 mi |
| Mediana / P50 | 1,70 mi |
| P75 | 3,01 mi |
| P90 | 6,00 mi |
| P99 | 18,24 mi |

### Distribuição das corridas por faixa

| Faixa | Corridas | Percentual |
|---|---:|---:|
| Curta (0–1 mi) | 3.316.638 | 26,18% |
| Média (1–3 mi) | 6.180.140 | 48,78% |
| Longa (3–7 mi) | 2.141.331 | 16,90% |
| Muito longa (7–15 mi) | 740.577 | 5,85% |
| Extrema (>15 mi) | 290.935 | 2,30% |

---

## 11. Análise de Desempenho

### Eficiência

A eficiência mede o aproveitamento médio dos processos utilizados:

```text
Eficiência = Speedup / Número de processos
```

Para a melhor execução:

```text
Eficiência = 2,73 / 12
Eficiência = 22,74%
```

A eficiência diminui conforme o número de processos aumenta porque nem todo o programa é paralelizável e porque existem custos adicionais de coordenação. Com 2 processos, a eficiência ficou em **96,89%**; com 12 processos, apesar do menor tempo total, a eficiência caiu para **22,74%**.

### Lei de Amdahl

A Lei de Amdahl explica que o speedup máximo é limitado pela parte do programa que continua sequencial:

```text
Speedup máximo = 1 / (S + (1 - S) / P)
```

Onde:

- `S` é a fração sequencial do programa;
- `P` é o número de processos.

Mesmo quando grande parte do processamento é paralelizável, etapas como leitura, divisão de intervalos, criação dos processos, combinação das parciais, cálculo dos percentis e gravação dos resultados limitam o ganho máximo.

### Observação sobre escalabilidade

O resultado com **8 processos** foi ligeiramente pior que com **4 processos**, o que pode ocorrer em benchmarks reais. Mais processos podem aumentar a concorrência por disco, cache e memória, além de gerar overhead de gerenciamento. Por isso, o desempenho paralelo nem sempre cresce proporcionalmente ao número de processos.

---

## 12. Gráficos Complementares

Além dos gráficos principais de desempenho apresentados na seção de speedup, o benchmark também gerou gráficos sobre a distribuição e as estatísticas das corridas.

### Distribuição das corridas

![Distribuição das corridas](grafico_distribuicao.png)

### Estatísticas das distâncias

![Estatísticas das distâncias](grafico_estatisticas.png)

---

## 13. Conclusão

A implementação paralela produziu os mesmos resultados estatísticos da execução sequencial, confirmando que a divisão do trabalho entre processos preservou a correção dos cálculos.

Em desempenho, o benchmark mostrou melhora clara em relação à execução com 1 processo. O tempo médio caiu de **282,07 s** para **103,35 s**, usando **12 processos**. Isso representa speedup de **2,73×** e redução de aproximadamente **63,36%** no tempo de execução.

O resultado também mostra que paralelismo não escala de forma perfeitamente linear. A configuração com 8 processos foi pior que a de 4 processos, e a eficiência diminuiu conforme mais processos foram adicionados. Isso indica que o projeto está sujeito a gargalos reais, principalmente I/O de disco, overhead de multiprocessamento e etapas sequenciais de redução.

Assim, o projeto demonstra tanto a vantagem prática do processamento paralelo quanto suas limitações, oferecendo uma análise coerente de speedup, eficiência e saturação de desempenho.
