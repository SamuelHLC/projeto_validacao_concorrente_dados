"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão PARALELA (multiprocessing)
  Programação Concorrente e Distribuída
=============================================================
  Estratégia Map-Reduce:
    - Divide o CSV em N chunks iguais
    - Cada processo filho processa seu chunk (MAP)
    - O processo principal combina os resultados (REDUCE)

  Métricas calculadas:
    - Soma total das distâncias
    - Média das corridas
    - Maior corrida / Menor corrida
    - Mediana e percentis (P25, P75, P90, P99)
    - Desvio padrão
    - Distribuição por faixas de distância

  Uso:
    python taxi_parallel.py                  # todos os núcleos
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

FAIXAS = [
    (0.0,  1.0,  "Curta      (0 – 1 mi)"),
    (1.0,  3.0,  "Média      (1 – 3 mi)"),
    (3.0,  7.0,  "Longa      (3 – 7 mi)"),
    (7.0,  15.0, "Muito longa(7 – 15 mi)"),
    (15.0, float("inf"), "Extrema    (> 15 mi)"),
]
# ──────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════
#  Funções de cálculo (reutilizáveis e testáveis)
# ══════════════════════════════════════════════════════════

def calcular_soma(distancias: list) -> float:
    """Soma total das distâncias."""
    return sum(distancias)


def calcular_media(distancias: list) -> float:
    """Média aritmética das distâncias."""
    return calcular_soma(distancias) / len(distancias) if distancias else 0.0


def calcular_maior_corrida(distancias: list) -> float:
    """Maior distância registrada."""
    return max(distancias) if distancias else 0.0


def calcular_menor_corrida(distancias: list) -> float:
    """Menor distância válida (> 0)."""
    return min(distancias) if distancias else 0.0


def calcular_desvio_padrao(distancias: list, media: float) -> float:
    """Desvio padrão populacional."""
    if len(distancias) < 2:
        return 0.0
    variancia = sum((d - media) ** 2 for d in distancias) / len(distancias)
    return math.sqrt(variancia)


def calcular_percentil(distancias_ordenadas: list, p: float) -> float:
    """Percentil p (0–100) com interpolação linear. Lista deve estar ordenada."""
    n = len(distancias_ordenadas)
    if n == 0:
        return 0.0
    indice   = (p / 100) * (n - 1)
    inferior = int(indice)
    superior = min(inferior + 1, n - 1)
    fracao   = indice - inferior
    return (distancias_ordenadas[inferior]
            + fracao * (distancias_ordenadas[superior] - distancias_ordenadas[inferior]))


def calcular_mediana(distancias_ordenadas: list) -> float:
    """Mediana (P50). Lista deve estar ordenada."""
    return calcular_percentil(distancias_ordenadas, 50)


def calcular_distribuicao(distancias: list) -> dict:
    """Conta corridas em cada faixa de distância."""
    contagens = {label: 0 for _, _, label in FAIXAS}
    for d in distancias:
        for baixo, alto, label in FAIXAS:
            if baixo < d <= alto:
                contagens[label] += 1
                break
    return contagens


# ══════════════════════════════════════════════════════════
#  Processamento Map-Reduce
# ══════════════════════════════════════════════════════════

def processar_chunk(linhas: list) -> dict:
    """
    MAP — executado em cada processo filho.
    Retorna estatísticas parciais do chunk, incluindo lista de distâncias
    para posterior cálculo de percentis no REDUCE.
    """
    soma       = 0.0
    contagem   = 0
    maior      = float("-inf")
    menor      = float("inf")
    distancias = []

    for row in linhas:
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


def worker(args: tuple) -> dict:
    """Wrapper para Pool.map — recebe (chunk_id, linhas)."""
    _chunk_id, linhas = args
    return processar_chunk(linhas)


def combinar_resultados(parciais: list) -> dict:
    """
    REDUCE — combina todos os resultados parciais em métricas finais.
    """
    soma_total        = 0.0
    contagem_total    = 0
    maior_global      = float("-inf")
    menor_global      = float("inf")
    todas_distancias  = []

    for p in parciais:
        soma_total        += p["soma"]
        contagem_total    += p["contagem"]
        todas_distancias  += p.get("distancias", [])
        if p["maior"] > maior_global:
            maior_global = p["maior"]
        if p["menor"] < menor_global:
            menor_global = p["menor"]

    media = calcular_media(todas_distancias)

    # Ordena uma única vez — base para mediana e todos os percentis
    todas_distancias.sort()

    desvio  = calcular_desvio_padrao(todas_distancias, media)
    mediana = calcular_mediana(todas_distancias)
    p25     = calcular_percentil(todas_distancias, 25)
    p75     = calcular_percentil(todas_distancias, 75)
    p90     = calcular_percentil(todas_distancias, 90)
    p99     = calcular_percentil(todas_distancias, 99)

    distribuicao = calcular_distribuicao(todas_distancias)

    return {
        # ── Métricas principais ──────────────────────────
        "soma_total":     round(soma_total,   4),
        "media":          round(media,         4),
        "maior_corrida":  round(maior_global,  4),
        "menor_corrida":  round(menor_global,  4),
        "total_corridas": contagem_total,
        # ── Métricas adicionais ──────────────────────────
        "desvio_padrao":  round(desvio,  4),
        "mediana":        round(mediana, 4),
        "percentil_25":   round(p25,     4),
        "percentil_75":   round(p75,     4),
        "percentil_90":   round(p90,     4),
        "percentil_99":   round(p99,     4),
        "distribuicao":   distribuicao,
    }


# ══════════════════════════════════════════════════════════
#  I/O e divisão
# ══════════════════════════════════════════════════════════

def ler_csv(csv_path: str) -> list:
    """Lê todo o CSV e retorna lista de dicionários."""
    linhas = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linhas.append(row)
    return linhas


def dividir_em_chunks(linhas: list, n: int) -> list:
    """Divide a lista em N partes aproximadamente iguais."""
    tamanho = ceil(len(linhas) / n)
    return [linhas[i : i + tamanho] for i in range(0, len(linhas), tamanho)]


def executar_paralelo(
    csv_path: str, num_processos: int
) -> tuple:
    """
    Executa o pipeline completo.
    Retorna: (resultado, tempo_total, tempo_leitura, tempo_processamento)
    """
    # Leitura (sequencial, única vez)
    t0 = time.perf_counter()
    todas_as_linhas = ler_csv(csv_path)
    t1 = time.perf_counter()
    tempo_leitura = t1 - t0

    # Divisão
    chunks = dividir_em_chunks(todas_as_linhas, num_processos)
    args   = [(i, chunk) for i, chunk in enumerate(chunks)]

    # MAP paralelo
    t2 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        resultados_parciais = pool.map(worker, args)
    t3 = time.perf_counter()
    tempo_processamento = t3 - t2

    # REDUCE
    resultado = combinar_resultados(resultados_parciais)

    tempo_total = t3 - t0
    return resultado, tempo_total, tempo_leitura, tempo_processamento


# ══════════════════════════════════════════════════════════
#  Exibição
# ══════════════════════════════════════════════════════════

def exibir_resultados(resultado: dict, num_processos: int,
                      tempo_total: float, tempo_leitura: float,
                      tempo_proc: float) -> None:
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
    print(f"  {'Tempo de leitura CSV':<30}: {tempo_leitura:>13.4f} s")
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

    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        print("Coloque o arquivo CSV na mesma pasta ou ajuste CSV_FILE.")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    resultado, tempo_total, tempo_leitura, tempo_proc = executar_paralelo(
        csv_path, num_processos
    )

    exibir_resultados(resultado, num_processos, tempo_total, tempo_leitura, tempo_proc)

    # Salva JSON individual
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