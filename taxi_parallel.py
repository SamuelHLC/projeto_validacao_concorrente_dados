"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão PARALELA (multiprocessing)
  Programação Concorrente e Distribuída
=============================================================
  Estratégia:
    - Divide o CSV em N chunks iguais
    - Cada processo filho processa seu chunk independentemente
    - O processo principal combina os resultados (reduce)

  Uso:
    python taxi_parallel.py                  # usa todos os núcleos
    python taxi_parallel.py --processos 4    # usa 4 processos
=============================================================
"""

import csv
import time
import json
import os
import sys
import argparse
import multiprocessing as mp
from math import ceil

# ─── Configuração ─────────────────────────────────────────
CSV_FILE         = "yellow_tripdata_2015-01.csv"
DIST_COL         = "trip_distance"
RESULTS_DIR      = "resultados_paralelos"
# ──────────────────────────────────────────────────────────


# ── Funções de processamento ──────────────────────────────

def processar_chunk(linhas: list[dict]) -> dict:
    """Processa um chunk de linhas — executa em processo filho."""
    soma     = 0.0
    contagem = 0
    maior    = float("-inf")
    menor    = float("inf")

    for row in linhas:
        try:
            dist = float(row[DIST_COL])
        except (ValueError, KeyError):
            continue

        if dist <= 0:
            continue

        soma     += dist
        contagem += 1
        if dist > maior:
            maior = dist
        if dist < menor:
            menor = dist

    return {
        "soma":     soma,
        "contagem": contagem,
        "maior":    maior if contagem > 0 else 0,
        "menor":    menor if contagem > 0 else 0,
    }


def worker(args: tuple) -> dict:
    """Wrapper para Pool.map — recebe (chunk_id, linhas)."""
    chunk_id, linhas = args
    resultado = processar_chunk(linhas)
    return resultado


def combinar_resultados(parciais: list[dict]) -> dict:
    """Fase Reduce: combina todos os resultados parciais."""
    soma_total     = 0.0
    contagem_total = 0
    maior_global   = float("-inf")
    menor_global   = float("inf")

    for p in parciais:
        soma_total     += p["soma"]
        contagem_total += p["contagem"]
        if p["maior"] > maior_global:
            maior_global = p["maior"]
        if p["menor"] < menor_global:
            menor_global = p["menor"]

    media = soma_total / contagem_total if contagem_total > 0 else 0

    return {
        "soma_total":     round(soma_total, 4),
        "media":          round(media, 4),
        "maior_corrida":  round(maior_global, 4),
        "menor_corrida":  round(menor_global, 4),
        "total_corridas": contagem_total,
    }


# ── Leitura e divisão do CSV ──────────────────────────────

def ler_csv(csv_path: str) -> list[dict]:
    """Lê todo o CSV e retorna lista de dicionários."""
    linhas = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linhas.append(row)
    return linhas


def dividir_em_chunks(linhas: list, n: int) -> list[list]:
    """Divide a lista em N partes aproximadamente iguais."""
    tamanho_chunk = ceil(len(linhas) / n)
    return [linhas[i : i + tamanho_chunk] for i in range(0, len(linhas), tamanho_chunk)]


# ── Execução paralela ─────────────────────────────────────

def executar_paralelo(csv_path: str, num_processos: int) -> tuple[dict, float, float, float]:
    """
    Retorna: (resultado, tempo_total, tempo_leitura, tempo_processamento)
    """
    # --- Leitura do arquivo (sequencial, única vez) ---
    t0 = time.perf_counter()
    todas_as_linhas = ler_csv(csv_path)
    t1 = time.perf_counter()
    tempo_leitura = t1 - t0

    # --- Divisão em chunks ---
    chunks = dividir_em_chunks(todas_as_linhas, num_processos)
    args   = [(i, chunk) for i, chunk in enumerate(chunks)]

    # --- Processamento paralelo ---
    t2 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        resultados_parciais = pool.map(worker, args)
    t3 = time.perf_counter()
    tempo_processamento = t3 - t2

    # --- Combine (Reduce) ---
    resultado = combinar_resultados(resultados_parciais)

    tempo_total = t3 - t0
    return resultado, tempo_total, tempo_leitura, tempo_processamento


# ── Main ──────────────────────────────────────────────────

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

    print("=" * 60)
    print(f"  NYC Yellow Taxi  —  Processamento PARALELO")
    print(f"  Processos utilizados: {num_processos}")
    print("=" * 60)

    resultado, tempo_total, tempo_leitura, tempo_proc = executar_paralelo(
        csv_path, num_processos
    )

    print(f"\n  Total de corridas válidas : {resultado['total_corridas']:,}")
    print(f"  Soma total de distâncias  : {resultado['soma_total']:,.4f} milhas")
    print(f"  Média das corridas        : {resultado['media']:.4f} milhas")
    print(f"  Maior corrida             : {resultado['maior_corrida']:.4f} milhas")
    print(f"  Menor corrida             : {resultado['menor_corrida']:.4f} milhas")
    print(f"\n  Tempo de leitura CSV      : {tempo_leitura:.4f} s")
    print(f"  Tempo de processamento    : {tempo_proc:.4f} s")
    print(f"  Tempo TOTAL               : {tempo_total:.4f} s")
    print("=" * 60)

    # Salva resultado individual
    saida = {
        **resultado,
        "num_processos":          num_processos,
        "tempo_total_segundos":   round(tempo_total,  6),
        "tempo_leitura_segundos": round(tempo_leitura, 6),
        "tempo_proc_segundos":    round(tempo_proc,   6),
    }
    arquivo_saida = os.path.join(
        RESULTS_DIR, f"resultado_p{num_processos:02d}.json"
    )
    with open(arquivo_saida, "w") as f:
        json.dump(saida, f, indent=2)
    print(f"\n  Resultado salvo em: {arquivo_saida}")


if __name__ == "__main__":
    main()
