# NYC Yellow Taxi Trip Data
## Programação Concorrente e Distribuída

---

## Estrutura dos arquivos

```
taxi_sequential.py    ← versão sequencial (1 processo)
taxi_parallel.py      ← versão paralela (N processos, multiprocessing)
taxi_benchmark.py     ← roda tudo e gera os 4 gráficos automaticamente
README.md
```

---

## Pré-requisitos

```bash
pip install matplotlib
```

Python padrão já inclui: `csv`, `multiprocessing`, `json`, `time`, `os`.

---

## Como usar

### 1. Coloque o CSV na mesma pasta dos scripts
Baixe `yellow_tripdata_2015-01.csv` de:
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data

### 2. Versão Sequencial
```bash
python taxi_sequential.py
```
Saída: `sequential_results.json`

### 3. Versão Paralela
```bash
# Usa todos os núcleos da máquina
python taxi_parallel.py

# Especifica número de processos
python taxi_parallel.py --processos 4
python taxi_parallel.py -p 8
```
Saída: `resultados_paralelos/resultado_p04.json`

### 4. Benchmark completo + Gráficos
```bash
# Testa automaticamente: 1, 2, 4, 8 processos
python taxi_benchmark.py

# Limita ao máximo desejado
python taxi_benchmark.py --max-processos 8
python taxi_benchmark.py -m 16
```
Gráficos gerados em `graficos_benchmark/`:
- `grafico_tempo.png`
- `grafico_speedup.png`
- `grafico_eficiencia.png`
- `grafico_barras_tempo.png`

---

## Métricas calculadas

| Métrica | Descrição |
|---|---|
| **Soma total** | Soma de todas as distâncias (em milhas) |
| **Média** | Distância média por corrida |
| **Maior corrida** | Distância máxima registrada |
| **Menor corrida** | Distância mínima válida (> 0) |
| **Total de corridas** | Número de registros válidos processados |

---

## Métricas de desempenho

### Speedup
```
Speedup(p) = T(1) / T(p)
```
Onde T(1) = tempo com 1 processo, T(p) = tempo com p processos.

### Eficiência
```
Eficiência(p) = Speedup(p) / p × 100%
```
Eficiência ideal = 100%. Na prática cai devido a overhead de comunicação.

---

## Estratégia de paralelismo

```
                    ┌─────────────┐
                    │  Lê CSV     │  (sequencial, 1 vez)
                    └──────┬──────┘
                           │  divide em N chunks iguais
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
     ┌──────────┐    ┌──────────┐    ┌──────────┐
     │ Processo │    │ Processo │    │ Processo │
     │    1     │    │    2     │    │    N     │
     │ (chunk1) │    │ (chunk2) │    │ (chunkN) │
     └────┬─────┘    └────┬─────┘    └────┬─────┘
          └───────────────┴───────────────┘
                           │  REDUCE
                    ┌──────▼──────┐
                    │  Combina    │
                    │  resultados │
                    └─────────────┘
```

---

## Exemplo de saída esperada

```
=======================================================
  NYC Yellow Taxi  —  Processamento PARALELO
  Processos utilizados: 4
=======================================================

  Total de corridas válidas : 12,748,986
  Soma total de distâncias  : 30,245,817.6543 milhas
  Média das corridas        : 2.3724 milhas
  Maior corrida             : 810.0000 milhas
  Menor corrida             : 0.0100 milhas

  Tempo de leitura CSV      : 45.2341 s
  Tempo de processamento    : 12.4567 s
  Tempo TOTAL               : 57.6908 s
```
