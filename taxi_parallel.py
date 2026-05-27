"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão PARALELA (multiprocessing)
  Programação Concorrente e Distribuída
=============================================================
  Filtro aplicado:
    - Passo 1: calcula o P99 sequencialmente (passagem rápida)
    - Passo 2: MAP paralelo com filtro dist >= DIST_MIN e <= P99
    - Passo 3: REDUCE combina os parciais

  Uso:
    python taxi_parallel.py
    python taxi_parallel.py --processos 4
    python taxi_parallel.py -p 8
=============================================================
"""

import csv
import time
import json
import os
import sys
import math
import argparse
import multiprocessing as mp
from math import ceil

# ─── Configuração ─────────────────────────────────────────
CSV_FILE    = "yellow_tripdata_2015-01.csv"
DIST_COL    = "trip_distance"
RESULTS_DIR = "resultados_paralelos"
DIST_MIN    = 0.1          # mínimo absoluto

FAIXAS = [
    (0.0,  1.0,  "Curta      (0 – 1 mi)"),
    (1.0,  3.0,  "Média      (1 – 3 mi)"),
    (3.0,  7.0,  "Longa      (3 – 7 mi)"),
    (7.0,  15.0, "Muito longa(7 – 15 mi)"),
    (15.0, float("inf"), "Extrema    (> 15 mi)"),
]
# ──────────────────────────────────────────────────────────


# ── Funções de cálculo ────────────────────────────────────

def calcular_media(distancias):
    return sum(distancias) / len(distancias) if distancias else 0.0

def calcular_desvio_padrao(distancias, media):
    if len(distancias) < 2:
        return 0.0
    return math.sqrt(sum((d - media) ** 2 for d in distancias) / len(distancias))

def calcular_percentil(ordenadas, p):
    n = len(ordenadas)
    if n == 0:
        return 0.0
    idx = (p / 100) * (n - 1)
    lo  = int(idx)
    hi  = min(lo + 1, n - 1)
    return ordenadas[lo] + (idx - lo) * (ordenadas[hi] - ordenadas[lo])

def calcular_distribuicao(distancias):
    contagens = {label: 0 for _, _, label in FAIXAS}
    for d in distancias:
        for baixo, alto, label in FAIXAS:
            if baixo < d <= alto:
                contagens[label] += 1
                break
    return contagens


# ── Filtro P99 (sequencial, rápido) ──────────────────────

def calcular_limite_p99(csv_path):
    """Primeira passagem leve: só coleta distâncias para calcular P99."""
    distancias = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dist = float(row[DIST_COL])
            except (ValueError, KeyError):
                continue
            if dist >= DIST_MIN:
                distancias.append(dist)
    distancias.sort()
    return calcular_percentil(distancias, 99)


# ── Map-Reduce ────────────────────────────────────────────

def processar_chunk(args):
    """MAP — executado em cada processo filho com filtro aplicado."""
    linhas, limite_p99 = args
    soma       = 0.0
    contagem   = 0
    removidos  = 0
    maior      = float("-inf")
    menor      = float("inf")
    distancias = []

    for row in linhas:
        try:
            dist = float(row[DIST_COL])
        except (ValueError, KeyError):
            continue

        if dist < DIST_MIN:
            continue
        if dist > limite_p99:
            removidos += 1
            continue

        soma     += dist
        contagem += 1
        distancias.append(dist)
        if dist > maior: maior = dist
        if dist < menor: menor = dist

    return {
        "soma":       soma,
        "contagem":   contagem,
        "removidos":  removidos,
        "maior":      maior if contagem > 0 else 0.0,
        "menor":      menor if contagem > 0 else 0.0,
        "distancias": distancias,
    }


def combinar_resultados(parciais, limite_p99):
    """REDUCE — combina todos os resultados parciais."""
    soma_total   = 0.0
    contagem_total = 0
    removidos_total = 0
    maior_global = float("-inf")
    menor_global = float("inf")
    todas        = []

    for p in parciais:
        soma_total      += p["soma"]
        contagem_total  += p["contagem"]
        removidos_total += p["removidos"]
        todas           += p["distancias"]
        if p["maior"] > maior_global: maior_global = p["maior"]
        if p["menor"] < menor_global: menor_global = p["menor"]

    media = calcular_media(todas)
    todas.sort()

    return {
        "soma_total":         round(soma_total, 4),
        "media":              round(media, 4),
        "maior_corrida":      round(maior_global, 4),
        "menor_corrida":      round(menor_global, 4),
        "total_corridas":     contagem_total,
        "outliers_removidos": removidos_total,
        "limite_p99":         round(limite_p99, 4),
        "desvio_padrao":      round(calcular_desvio_padrao(todas, media), 4),
        "mediana":            round(calcular_percentil(todas, 50), 4),
        "percentil_25":       round(calcular_percentil(todas, 25), 4),
        "percentil_75":       round(calcular_percentil(todas, 75), 4),
        "percentil_90":       round(calcular_percentil(todas, 90), 4),
        "distribuicao":       calcular_distribuicao(todas),
    }


# ── I/O ───────────────────────────────────────────────────

def ler_csv(csv_path):
    linhas = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linhas.append(row)
    return linhas

def dividir_em_chunks(linhas, n):
    sz = ceil(len(linhas) / n)
    return [linhas[i : i + sz] for i in range(0, len(linhas), sz)]


# ── Execução ──────────────────────────────────────────────

def executar_paralelo(csv_path, num_processos):
    # Passo 1 — P99 (sequencial, rápido)
    print("  [1/3] Calculando limite P99...", end=" ", flush=True)
    t0 = time.perf_counter()
    limite_p99 = calcular_limite_p99(csv_path)
    print(f"P99 = {limite_p99:.4f} milhas")

    # Passo 2 — leitura
    print("  [2/3] Lendo CSV...", end=" ", flush=True)
    todas_as_linhas = ler_csv(csv_path)
    t1 = time.perf_counter()
    tempo_leitura = t1 - t0
    print(f"OK  ({len(todas_as_linhas):,} linhas, {tempo_leitura:.2f}s)")

    # Passo 3 — MAP paralelo
    print(f"  [3/3] Processando com {num_processos} processo(s)...", end=" ", flush=True)
    chunks = dividir_em_chunks(todas_as_linhas, num_processos)
    args   = [(chunk, limite_p99) for chunk in chunks]

    t2 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(processar_chunk, args)
    t3 = time.perf_counter()
    tempo_processamento = t3 - t2

    resultado = combinar_resultados(parciais, limite_p99)
    print(f"OK  ({resultado['outliers_removidos']:,} outliers removidos)")

    tempo_total = t3 - t0
    return resultado, tempo_total, tempo_leitura, tempo_processamento


# ── Exibição ──────────────────────────────────────────────

def exibir_resultados(resultado, num_processos, tempo_total, tempo_leitura, tempo_proc):
    larg = 62
    sep  = "=" * larg
    print(sep)
    print("  NYC Yellow Taxi  —  Processamento PARALELO")
    print(f"  Processos: {num_processos}  |  Filtro: {DIST_MIN} mi – P99 ({resultado['limite_p99']} mi)")
    print(sep)

    print(f"\n  {'Total de corridas (pós-filtro)':<32}: {resultado['total_corridas']:>12,}")
    print(f"  {'Outliers removidos':<32}: {resultado['outliers_removidos']:>12,}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  DISTÂNCIAS (milhas)")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Soma total':<32}: {resultado['soma_total']:>12,.4f}")
    print(f"  {'Média':<32}: {resultado['media']:>12.4f}")
    print(f"  {'Maior corrida':<32}: {resultado['maior_corrida']:>12.4f}")
    print(f"  {'Menor corrida':<32}: {resultado['menor_corrida']:>12.4f}")
    print(f"  {'Desvio padrão':<32}: {resultado['desvio_padrao']:>12.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  PERCENTIS")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'P25 (1º quartil)':<32}: {resultado['percentil_25']:>12.4f}")
    print(f"  {'P50 (mediana)':<32}: {resultado['mediana']:>12.4f}")
    print(f"  {'P75 (3º quartil)':<32}: {resultado['percentil_75']:>12.4f}")
    print(f"  {'P90':<32}: {resultado['percentil_90']:>12.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  DISTRIBUIÇÃO POR FAIXA")
    print(f"  {'─' * (larg - 4)}")
    total = resultado["total_corridas"]
    for label, qtd in resultado["distribuicao"].items():
        pct   = qtd / total * 100 if total > 0 else 0
        barra = "█" * int(pct / 2)
        print(f"  {label}: {qtd:>10,}  ({pct:5.1f}%)  {barra}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  DESEMPENHO")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Tempo de leitura CSV':<32}: {tempo_leitura:>11.4f} s")
    print(f"  {'Tempo de processamento':<32}: {tempo_proc:>11.4f} s")
    print(f"  {'Tempo TOTAL':<32}: {tempo_total:>11.4f} s")
    print(sep)


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--processos", "-p", type=int, default=mp.cpu_count())
    args = parser.parse_args()
    num_processos = max(1, args.processos)

    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    resultado, tempo_total, tempo_leitura, tempo_proc = executar_paralelo(
        csv_path, num_processos
    )
    exibir_resultados(resultado, num_processos, tempo_total, tempo_leitura, tempo_proc)

    saida = {
        **{k: v for k, v in resultado.items() if k != "distancias"},
        "num_processos":          num_processos,
        "tempo_total_segundos":   round(tempo_total,   6),
        "tempo_leitura_segundos": round(tempo_leitura, 6),
        "tempo_proc_segundos":    round(tempo_proc,    6),
    }
    arquivo_saida = os.path.join(RESULTS_DIR, f"resultado_p{num_processos:02d}.json")
    with open(arquivo_saida, "w") as f:
        json.dump(saida, f, indent=2, ensure_ascii=False)
    print(f"\n  Resultado salvo em: {arquivo_saida}")


if __name__ == "__main__":
    main()
