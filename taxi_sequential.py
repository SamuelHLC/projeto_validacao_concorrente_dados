"""
=============================================================
  NYC Yellow Taxi Trip Data - Versão SEQUENCIAL
  Programação Concorrente e Distribuída
=============================================================
  Métricas calculadas:
    - Soma total das distâncias
    - Média das corridas
    - Maior corrida
    - Menor corrida (> 0)
=============================================================
"""

import csv
import time
import json
import os
import sys

# ─── Configuração ─────────────────────────────────────────
CSV_FILE  = "yellow_tripdata_2015-01.csv"   # ajuste o caminho se necessário
DIST_COL  = "trip_distance"
RESULTS_FILE = "sequential_results.json"
# ──────────────────────────────────────────────────────────


def processar_chunk(linhas: list[dict]) -> dict:
    """Processa uma lista de linhas e retorna estatísticas parciais."""
    soma       = 0.0
    contagem   = 0
    maior      = float("-inf")
    menor      = float("inf")

    for row in linhas:
        try:
            dist = float(row[DIST_COL])
        except (ValueError, KeyError):
            continue

        if dist <= 0:          # ignora distâncias inválidas / zero
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


def combinar_resultados(parciais: list[dict]) -> dict:
    """Combina N resultados parciais em um resultado final."""
    soma_total = 0.0
    contagem_total = 0
    maior_global = float("-inf")
    menor_global = float("inf")

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


def executar_sequencial(csv_path: str) -> tuple[dict, float]:
    """Lê o CSV e processa tudo em sequência (1 processo)."""
    inicio = time.perf_counter()

    todas_as_linhas: list[dict] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            todas_as_linhas.append(row)

    # Processa tudo de uma vez (sem divisão)
    parcial = processar_chunk(todas_as_linhas)
    resultado = combinar_resultados([parcial])

    tempo = time.perf_counter() - inicio
    return resultado, tempo


def main():
    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        print("Coloque o arquivo CSV na mesma pasta do script ou ajuste CSV_FILE.")
        sys.exit(1)

    print("=" * 55)
    print("  NYC Yellow Taxi  —  Processamento SEQUENCIAL")
    print("=" * 55)

    resultado, tempo = executar_sequencial(csv_path)

    print(f"\n  Total de corridas válidas : {resultado['total_corridas']:,}")
    print(f"  Soma total de distâncias  : {resultado['soma_total']:,.4f} milhas")
    print(f"  Média das corridas        : {resultado['media']:.4f} milhas")
    print(f"  Maior corrida             : {resultado['maior_corrida']:.4f} milhas")
    print(f"  Menor corrida             : {resultado['menor_corrida']:.4f} milhas")
    print(f"\n  Tempo de execução         : {tempo:.4f} segundos")
    print("=" * 55)

    # Salva resultado para comparação posterior
    saida = {**resultado, "tempo_segundos": round(tempo, 6), "num_processos": 1}
    with open(RESULTS_FILE, "w") as f:
        json.dump(saida, f, indent=2)
    print(f"\n  Resultados salvos em: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
