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
    - Mediana e percentis (P25, P75, P90, P99)
    - Desvio padrão
    - Distribuição por faixas de distância
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

# Faixas de distância (em milhas) para distribuição
FAIXAS = [
    (0.0,  1.0,  "Curta      (0 – 1 mi)"),
    (1.0,  3.0,  "Média      (1 – 3 mi)"),
    (3.0,  7.0,  "Longa      (3 – 7 mi)"),
    (7.0,  15.0, "Muito longa(7 – 15 mi)"),
    (15.0, float("inf"), "Extrema    (> 15 mi)"),
]
# ──────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════
#  Funções de cálculo
# ══════════════════════════════════════════════════════════

def calcular_soma(distancias: list[float]) -> float:
    """Retorna a soma total das distâncias."""
    return sum(distancias)


def calcular_media(distancias: list[float]) -> float:
    """Retorna a média aritmética das distâncias."""
    if not distancias:
        return 0.0
    return calcular_soma(distancias) / len(distancias)


def calcular_maior_corrida(distancias: list[float]) -> float:
    """Retorna a maior distância registrada."""
    return max(distancias) if distancias else 0.0


def calcular_menor_corrida(distancias: list[float]) -> float:
    """Retorna a menor distância válida (> 0)."""
    return min(distancias) if distancias else 0.0


def calcular_desvio_padrao(distancias: list[float], media: float) -> float:
    """Desvio padrão populacional das distâncias."""
    if len(distancias) < 2:
        return 0.0
    variancia = sum((d - media) ** 2 for d in distancias) / len(distancias)
    return math.sqrt(variancia)


def calcular_percentil(distancias_ordenadas: list[float], p: float) -> float:
    """
    Percentil p (0–100) usando interpolação linear.
    Requer a lista já ordenada.
    """
    n = len(distancias_ordenadas)
    if n == 0:
        return 0.0
    indice = (p / 100) * (n - 1)
    inferior = int(indice)
    superior = min(inferior + 1, n - 1)
    fracao   = indice - inferior
    return distancias_ordenadas[inferior] + fracao * (
        distancias_ordenadas[superior] - distancias_ordenadas[inferior]
    )


def calcular_mediana(distancias_ordenadas: list[float]) -> float:
    """Mediana (P50) da lista ordenada."""
    return calcular_percentil(distancias_ordenadas, 50)


def calcular_distribuicao(distancias: list[float]) -> dict:
    """Conta corridas em cada faixa de distância."""
    contagens = {label: 0 for _, _, label in FAIXAS}
    for d in distancias:
        for baixo, alto, label in FAIXAS:
            if baixo < d <= alto or (baixo == 0.0 and d > 0):
                if baixo < d <= alto:
                    contagens[label] += 1
                    break
    return contagens


# ══════════════════════════════════════════════════════════
#  Processamento principal
# ══════════════════════════════════════════════════════════

def processar_chunk(linhas: list[dict]) -> dict:
    """
    Processa uma lista de linhas e retorna estatísticas parciais.
    Coleta as distâncias individuais para cálculos de percentil.
    """
    soma       = 0.0
    contagem   = 0
    maior      = float("-inf")
    menor      = float("inf")
    distancias = []          # ← novo: guarda valores para percentis

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
        "distancias": distancias,     # ← novo
    }


def combinar_resultados(parciais: list[dict]) -> dict:
    """Combina N resultados parciais em um resultado final completo."""
    soma_total     = 0.0
    contagem_total = 0
    maior_global   = float("-inf")
    menor_global   = float("inf")
    todas_distancias: list[float] = []

    for p in parciais:
        soma_total         += p["soma"]
        contagem_total     += p["contagem"]
        todas_distancias   += p.get("distancias", [])
        if p["maior"] > maior_global:
            maior_global = p["maior"]
        if p["menor"] < menor_global:
            menor_global = p["menor"]

    media = calcular_media(todas_distancias)

    # Ordena uma única vez para todos os percentis
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
        "soma_total":      round(soma_total,    4),
        "media":           round(media,          4),
        "maior_corrida":   round(maior_global,   4),
        "menor_corrida":   round(menor_global,   4),
        "total_corridas":  contagem_total,
        # ── Métricas adicionais ──────────────────────────
        "desvio_padrao":   round(desvio,  4),
        "mediana":         round(mediana, 4),
        "percentil_25":    round(p25,     4),
        "percentil_75":    round(p75,     4),
        "percentil_90":    round(p90,     4),
        "percentil_99":    round(p99,     4),
        "distribuicao":    distribuicao,
    }


def executar_sequencial(csv_path: str) -> tuple[dict, float]:
    """Lê o CSV e processa tudo em sequência (1 processo)."""
    inicio = time.perf_counter()

    todas_as_linhas: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            todas_as_linhas.append(row)

    parcial   = processar_chunk(todas_as_linhas)
    resultado = combinar_resultados([parcial])

    tempo = time.perf_counter() - inicio
    return resultado, tempo


# ══════════════════════════════════════════════════════════
#  Exibição
# ══════════════════════════════════════════════════════════

def exibir_resultados(resultado: dict, tempo: float) -> None:
    larg = 60
    sep  = "=" * larg

    print(sep)
    print("  NYC Yellow Taxi  —  Processamento SEQUENCIAL")
    print(sep)

    print(f"\n{'  RESUMO DAS CORRIDAS':}")
    print(f"  {'Total de corridas válidas':<30}: {resultado['total_corridas']:>15,}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DISTÂNCIAS (milhas)'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Soma total':<30}: {resultado['soma_total']:>15,.4f}")
    print(f"  {'Média':<30}: {resultado['media']:>15.4f}")
    print(f"  {'Maior corrida':<30}: {resultado['maior_corrida']:>15.4f}")
    print(f"  {'Menor corrida':<30}: {resultado['menor_corrida']:>15.4f}")
    print(f"  {'Desvio padrão':<30}: {resultado['desvio_padrao']:>15.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'PERCENTIS'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'P25 (1º quartil)':<30}: {resultado['percentil_25']:>15.4f}")
    print(f"  {'P50 (mediana)':<30}: {resultado['mediana']:>15.4f}")
    print(f"  {'P75 (3º quartil)':<30}: {resultado['percentil_75']:>15.4f}")
    print(f"  {'P90':<30}: {resultado['percentil_90']:>15.4f}")
    print(f"  {'P99':<30}: {resultado['percentil_99']:>15.4f}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DISTRIBUIÇÃO POR FAIXA'}")
    print(f"  {'─' * (larg - 4)}")
    total = resultado["total_corridas"]
    for label, qtd in resultado["distribuicao"].items():
        pct = qtd / total * 100 if total > 0 else 0
        barra = "█" * int(pct / 2)
        print(f"  {label}: {qtd:>10,}  ({pct:5.1f}%)  {barra}")
    print()

    print(f"  {'─' * (larg - 4)}")
    print(f"  {'DESEMPENHO'}")
    print(f"  {'─' * (larg - 4)}")
    print(f"  {'Tempo de execução':<30}: {tempo:>14.4f} s")
    print(sep)


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

def main():
    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        print("Coloque o arquivo CSV na mesma pasta do script ou ajuste CSV_FILE.")
        sys.exit(1)

    resultado, tempo = executar_sequencial(csv_path)
    exibir_resultados(resultado, tempo)

    saida = {
        **{k: v for k, v in resultado.items() if k != "distancias"},
        "tempo_segundos": round(tempo, 6),
        "num_processos":  1,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(saida, f, indent=2, ensure_ascii=False)
    print(f"\n  Resultados salvos em: {RESULTS_FILE}")


if __name__ == "__main__":
    main()