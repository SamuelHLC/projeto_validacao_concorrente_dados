"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão PARALELA (multiprocessing)
  Programação Concorrente e Distribuída
=============================================================
  CORREÇÃO: workers leem diretamente do disco (sem pickle de dados)
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

FAIXAS = [
    (0.0,  1.0,  "Curta      (0 – 1 mi)"),
    (1.0,  3.0,  "Média      (1 – 3 mi)"),
    (3.0,  7.0,  "Longa      (3 – 7 mi)"),
    (7.0,  15.0, "Muito longa(7 – 15 mi)"),
    (15.0, float("inf"), "Extrema    (> 15 mi)"),
]
# ──────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════
#  Funções de cálculo (sem alteração)
# ══════════════════════════════════════════════════════════

def calcular_soma(distancias):
    return sum(distancias)

def calcular_media(distancias):
    return calcular_soma(distancias) / len(distancias) if distancias else 0.0

def calcular_maior_corrida(distancias):
    return max(distancias) if distancias else 0.0

def calcular_menor_corrida(distancias):
    return min(distancias) if distancias else 0.0

def calcular_desvio_padrao(distancias, media):
    if len(distancias) < 2:
        return 0.0
    variancia = sum((d - media) ** 2 for d in distancias) / len(distancias)
    return math.sqrt(variancia)

def calcular_percentil(distancias_ordenadas, p):
    n = len(distancias_ordenadas)
    if n == 0:
        return 0.0
    indice   = (p / 100) * (n - 1)
    inferior = int(indice)
    superior = min(inferior + 1, n - 1)
    fracao   = indice - inferior
    return (distancias_ordenadas[inferior]
            + fracao * (distancias_ordenadas[superior] - distancias_ordenadas[inferior]))

def calcular_mediana(distancias_ordenadas):
    return calcular_percentil(distancias_ordenadas, 50)

def calcular_distribuicao(distancias):
    contagens = {label: 0 for _, _, label in FAIXAS}
    for d in distancias:
        for baixo, alto, label in FAIXAS:
            if baixo < d <= alto:
                contagens[label] += 1
                break
    return contagens


# ══════════════════════════════════════════════════════════
#  NOVO: worker lê do disco diretamente
# ══════════════════════════════════════════════════════════

def worker(args: tuple) -> dict:
    """
    MAP — cada processo lê suas próprias linhas do CSV.
    Recebe apenas (linha_inicio, linha_fim) — números pequenos, sem pickle de dados.
    """
    linha_inicio, linha_fim = args

    soma       = 0.0
    contagem   = 0
    maior      = float("-inf")
    menor      = float("inf")
    distancias = []

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < linha_inicio:
                continue
            if i >= linha_fim:
                break
            try:
                dist = float(row[DIST_COL])
            except (ValueError, KeyError):
                continue
            if dist <= 0:
                continue

            soma     += dist
            contagem += 1
            distancias.append(dist)
            if dist > maior:
                maior = dist
            if dist < menor:
                menor = dist

    return {
        "soma":       soma,
        "contagem":   contagem,
        "maior":      maior if contagem > 0 else 0.0,
        "menor":      menor if contagem > 0 else 0.0,
        "distancias": distancias,
    }


def contar_linhas_csv(csv_path: str) -> int:
    """Conta linhas de dados (sem o header) de forma eficiente."""
    with open(csv_path, "rb") as f:
        total = sum(1 for _ in f)
    return total - 1  # desconta o header


# ══════════════════════════════════════════════════════════
#  REDUCE (sem alteração)
# ══════════════════════════════════════════════════════════

def combinar_resultados(parciais: list) -> dict:
    soma_total       = 0.0
    contagem_total   = 0
    maior_global     = float("-inf")
    menor_global     = float("inf")
    todas_distancias = []

    for p in parciais:
        soma_total       += p["soma"]
        contagem_total   += p["contagem"]
        todas_distancias += p.get("distancias", [])
        if p["maior"] > maior_global:
            maior_global = p["maior"]
        if p["menor"] < menor_global:
            menor_global = p["menor"]

    media = calcular_media(todas_distancias)
    todas_distancias.sort()

    desvio  = calcular_desvio_padrao(todas_distancias, media)
    mediana = calcular_mediana(todas_distancias)
    p25     = calcular_percentil(todas_distancias, 25)
    p75     = calcular_percentil(todas_distancias, 75)
    p90     = calcular_percentil(todas_distancias, 90)
    p99     = calcular_percentil(todas_distancias, 99)

    distribuicao = calcular_distribuicao(todas_distancias)

    return {
        "soma_total":     round(soma_total,  4),
        "media":          round(media,        4),
        "maior_corrida":  round(maior_global, 4),
        "menor_corrida":  round(menor_global, 4),
        "total_corridas": contagem_total,
        "desvio_padrao":  round(desvio,  4),
        "mediana":        round(mediana, 4),
        "percentil_25":   round(p25,     4),
        "percentil_75":   round(p75,     4),
        "percentil_90":   round(p90,     4),
        "percentil_99":   round(p99,     4),
        "distribuicao":   distribuicao,
    }


# ══════════════════════════════════════════════════════════
#  Pipeline principal
# ══════════════════════════════════════════════════════════

def executar_paralelo(csv_path: str, num_processos: int) -> tuple:
    # Conta linhas (rápido, lê só bytes)
    print(f"  [1/3] Contando linhas...", end=" ", flush=True)
    t0 = time.perf_counter()
    total_linhas = contar_linhas_csv(csv_path)
    t1 = time.perf_counter()
    print(f"OK  ({total_linhas:,} linhas, {t1-t0:.2f}s)")

    # Divide por índice de linha — só números, sem dados
    chunk = ceil(total_linhas / num_processos)
    args  = [
        (i * chunk, min((i + 1) * chunk, total_linhas))
        for i in range(num_processos)
    ]

    # MAP paralelo — cada worker abre o CSV por conta própria
    print(f"  [2/3] Processando com {num_processos} processo(s)...", end=" ", flush=True)
    t2 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(worker, args)
    t3 = time.perf_counter()
    print(f"OK  ({t3-t2:.2f}s)")

    # REDUCE
    print(f"  [3/3] Combinando resultados...", end=" ", flush=True)
    resultado = combinar_resultados(parciais)
    t4 = time.perf_counter()
    print(f"OK  ({t4-t3:.2f}s)")

    return resultado, t4 - t0, 0.0, t3 - t2


# ══════════════════════════════════════════════════════════
#  Exibição (sem alteração)
# ══════════════════════════════════════════════════════════

def exibir_resultados(resultado, num_processos, tempo_total, tempo_leitura, tempo_proc):
    larg = 62
    sep  = "=" * larg

    print(sep)
    print("  NYC Yellow Taxi  —  Processamento PARALELO")
    print(f"  Processos utilizados: {num_processos}")
    print(sep)

    print(f"\n  {'Total de corridas válidas':<30}: {resultado['total_corridas']:>14,}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DISTÂNCIAS (milhas)'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Soma total':<30}: {resultado['soma_total']:>14,.4f}")
    print(f"  {'Média':<30}: {resultado['media']:>14.4f}")
    print(f"  {'Maior corrida':<30}: {resultado['maior_corrida']:>14.4f}")
    print(f"  {'Menor corrida':<30}: {resultado['menor_corrida']:>14.4f}")
    print(f"  {'Desvio padrão':<30}: {resultado['desvio_padrao']:>14.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'PERCENTIS'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'P25 (1º quartil)':<30}: {resultado['percentil_25']:>14.4f}")
    print(f"  {'P50 (mediana)':<30}: {resultado['mediana']:>14.4f}")
    print(f"  {'P75 (3º quartil)':<30}: {resultado['percentil_75']:>14.4f}")
    print(f"  {'P90':<30}: {resultado['percentil_90']:>14.4f}")
    print(f"  {'P99':<30}: {resultado['percentil_99']:>14.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DISTRIBUIÇÃO POR FAIXA'}")
    print(f"  {'─' * (larg - 4)}")
    total = resultado["total_corridas"]
    for label, qtd in resultado["distribuicao"].items():
        pct   = qtd / total * 100 if total > 0 else 0
        barra = "█" * int(pct / 2)
        print(f"  {label}: {qtd:>10,}  ({pct:5.1f}%)  {barra}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DESEMPENHO'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Tempo de processamento':<30}: {tempo_proc:>13.4f} s")
    print(f"  {'Tempo TOTAL':<30}: {tempo_total:>13.4f} s")
    print(sep)


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Análise paralela do NYC Yellow Taxi Trip Data"
    )
    parser.add_argument(
        "--processos", "-p",
        type=int,
        default=mp.cpu_count(),
        help=f"Número de processos (padrão: {mp.cpu_count()} — todos os núcleos)",
    )
    args = parser.parse_args()
    num_processos = max(1, args.processos)

    if not os.path.exists(CSV_FILE):
        print(f"[ERRO] Arquivo não encontrado: {CSV_FILE}")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    resultado, tempo_total, tempo_leitura, tempo_proc = executar_paralelo(
        CSV_FILE, num_processos
    )

    exibir_resultados(resultado, num_processos, tempo_total, tempo_leitura, tempo_proc)

    saida = {
        **{k: v for k, v in resultado.items() if k != "distancias"},
        "num_processos":        num_processos,
        "tempo_total_segundos": round(tempo_total, 6),
        "tempo_proc_segundos":  round(tempo_proc,  6),
    }
    arquivo_saida = os.path.join(RESULTS_DIR, f"resultado_p{num_processos:02d}.json")
    with open(arquivo_saida, "w") as f:
        json.dump(saida, f, indent=2, ensure_ascii=False)
    print(f"\n  Resultado salvo em: {arquivo_saida}")


if __name__ == "__main__":
    main()
