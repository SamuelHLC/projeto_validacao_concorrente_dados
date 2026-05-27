"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão SEQUENCIAL
  Programação Concorrente e Distribuída
=============================================================
  Filtro aplicado:
    - Passo 1: lê todas as distâncias válidas (> 0)
    - Passo 2: calcula o P99 como limite superior
    - Passo 3: reprocessa mantendo apenas distâncias <= P99
    Isso remove ~1% de registros corrompidos (ex: 15 milhões mi)
=============================================================
"""

import csv
import time
import json
import os
import sys
import math

# ─── Configuração ─────────────────────────────────────────
CSV_FILE     = "yellow_tripdata_2015-01.csv"
DIST_COL     = "trip_distance"
RESULTS_FILE = "sequential_results.json"
DIST_MIN     = 0.1          # mínimo absoluto (ignora corridas < 0.1 mi)

FAIXAS = [
    (0.0,  1.0,  "Curta      (0 – 1 mi)"),
    (1.0,  3.0,  "Média      (1 – 3 mi)"),
    (3.0,  7.0,  "Longa      (3 – 7 mi)"),
    (7.0,  15.0, "Muito longa(7 – 15 mi)"),
    (15.0, float("inf"), "Extrema    (> 15 mi)"),
]
# ──────────────────────────────────────────────────────────


# ── Funções de cálculo ────────────────────────────────────

def calcular_soma(distancias):
    return sum(distancias)

def calcular_media(distancias):
    return calcular_soma(distancias) / len(distancias) if distancias else 0.0

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


# ── Filtro P99 ────────────────────────────────────────────

def calcular_limite_p99(csv_path):
    """Primeira passagem: coleta distâncias válidas e calcula P99."""
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
    return calcular_percentil(distancias, 99), len(distancias)


# ── Processamento ─────────────────────────────────────────

def executar_sequencial(csv_path):
    inicio = time.perf_counter()

    # Passo 1 — calcula o limite P99
    print("  [1/2] Calculando limite P99...", end=" ", flush=True)
    limite_p99, total_bruto = calcular_limite_p99(csv_path)
    print(f"P99 = {limite_p99:.4f} milhas  ({total_bruto:,} registros válidos brutos)")

    # Passo 2 — lê e filtra
    print("  [2/2] Processando com filtro...", end=" ", flush=True)
    distancias = []
    removidos  = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dist = float(row[DIST_COL])
            except (ValueError, KeyError):
                continue
            if dist < DIST_MIN:
                continue
            if dist > limite_p99:
                removidos += 1
                continue
            distancias.append(dist)

    print(f"OK  ({removidos:,} outliers removidos)")

    # Cálculos finais
    distancias.sort()
    media        = calcular_media(distancias)
    desvio       = calcular_desvio_padrao(distancias, media)
    distribuicao = calcular_distribuicao(distancias)

    resultado = {
        "soma_total":     round(calcular_soma(distancias), 4),
        "media":          round(media, 4),
        "maior_corrida":  round(max(distancias), 4),
        "menor_corrida":  round(min(distancias), 4),
        "total_corridas": len(distancias),
        "outliers_removidos": removidos,
        "limite_p99":     round(limite_p99, 4),
        "desvio_padrao":  round(desvio, 4),
        "mediana":        round(calcular_percentil(distancias, 50), 4),
        "percentil_25":   round(calcular_percentil(distancias, 25), 4),
        "percentil_75":   round(calcular_percentil(distancias, 75), 4),
        "percentil_90":   round(calcular_percentil(distancias, 90), 4),
        "distribuicao":   distribuicao,
    }

    tempo = time.perf_counter() - inicio
    return resultado, tempo


# ── Exibição ──────────────────────────────────────────────

def exibir_resultados(resultado, tempo):
    larg = 62
    sep  = "=" * larg
    print(sep)
    print("  NYC Yellow Taxi  —  Processamento SEQUENCIAL")
    print(f"  Filtro: distâncias entre {DIST_MIN} mi e P99 ({resultado['limite_p99']} mi)")
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
    print(f"  {'Tempo de execução':<32}: {tempo:>11.4f} s")
    print(sep)


# ── Main ──────────────────────────────────────────────────

def main():
    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    resultado, tempo = executar_sequencial(csv_path)
    exibir_resultados(resultado, tempo)

    saida = {**resultado, "tempo_segundos": round(tempo, 6), "num_processos": 1}
    with open(RESULTS_FILE, "w") as f:
        json.dump(saida, f, indent=2, ensure_ascii=False)
    print(f"\n  Resultados salvos em: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
