"""
=============================================================
  NYC Yellow Taxi Trip Data - BENCHMARK + GRÁFICOS
  Programação Concorrente e Distribuída
=============================================================
  Executa automaticamente com 1, 2, 4, 8 … processos
  e gera os gráficos:
    1. Tempo de execução por número de processos
    2. Speedup (real vs. ideal)
    3. Eficiência paralela

  Uso:
    python taxi_benchmark.py
    python taxi_benchmark.py --max-processos 8
=============================================================
"""

import csv
import time
import json
import os
import sys
import argparse
import multiprocessing as mp
from math import ceil, log2

import matplotlib
matplotlib.use("Agg")          # sem janela gráfica (roda em servidor/terminal)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ─── Configuração ─────────────────────────────────────────
CSV_FILE    = "yellow_tripdata_2015-01.csv"
DIST_COL    = "trip_distance"
OUTPUT_DIR  = "graficos_benchmark"
REPETICOES  = 3   # média de N execuções para reduzir variância
# ──────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════
#  Lógica de processamento (mesma do taxi_parallel.py)
# ══════════════════════════════════════════════════════════

def processar_chunk(linhas):
    soma = contagem = 0
    maior = float("-inf")
    menor = float("inf")
    for row in linhas:
        try:
            dist = float(row[DIST_COL])
        except (ValueError, KeyError):
            continue
        if dist <= 0:
            continue
        soma     += dist
        contagem += 1
        if dist > maior: maior = dist
        if dist < menor: menor = dist
    return {
        "soma":     soma,
        "contagem": contagem,
        "maior":    maior if contagem > 0 else 0,
        "menor":    menor if contagem > 0 else 0,
    }


def worker(args):
    _, linhas = args
    return processar_chunk(linhas)


def combinar(parciais):
    soma = contagem = 0
    maior = float("-inf")
    menor = float("inf")
    for p in parciais:
        soma     += p["soma"]
        contagem += p["contagem"]
        if p["maior"] > maior: maior = p["maior"]
        if p["menor"] < menor: menor = p["menor"]
    media = soma / contagem if contagem > 0 else 0
    return {
        "soma_total":     round(soma, 4),
        "media":          round(media, 4),
        "maior_corrida":  round(maior, 4),
        "menor_corrida":  round(menor, 4),
        "total_corridas": contagem,
    }


def ler_csv(csv_path):
    linhas = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linhas.append(row)
    return linhas


def dividir(linhas, n):
    sz = ceil(len(linhas) / n)
    return [linhas[i : i + sz] for i in range(0, len(linhas), sz)]


def executar(linhas_cache, num_processos):
    """Executa o processamento com num_processos e retorna o tempo."""
    chunks = dividir(linhas_cache, num_processos)
    args   = list(enumerate(chunks))

    t0 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(worker, args)
    t1 = time.perf_counter()

    combinar(parciais)   # inclui o tempo do reduce
    return time.perf_counter() - t0    # tempo de processamento (sem I/O)


# ══════════════════════════════════════════════════════════
#  Gráficos
# ══════════════════════════════════════════════════════════

CORES = ["#2196F3", "#4CAF50", "#FF5722", "#9C27B0",
         "#FF9800", "#00BCD4", "#E91E63", "#8BC34A"]

def _fig_base(titulo, xlabel, ylabel):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(titulo, fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    return fig, ax


def grafico_tempo(processos, tempos, pasta):
    fig, ax = _fig_base(
        "Tempo de Execução × Número de Processos",
        "Número de Processos",
        "Tempo (segundos)",
    )
    ax.plot(processos, tempos, marker="o", linewidth=2.5,
            color=CORES[0], markersize=8, label="Tempo real")
    for x, y in zip(processos, tempos):
        ax.annotate(f"{y:.2f}s", (x, y),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_tempo.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_speedup(processos, tempos, pasta):
    t1     = tempos[0]
    real   = [t1 / t for t in tempos]
    ideal  = processos[:]

    fig, ax = _fig_base(
        "Speedup × Número de Processos",
        "Número de Processos",
        "Speedup",
    )
    ax.plot(processos, ideal, linestyle="--", linewidth=1.5,
            color="gray", label="Speedup ideal (linear)")
    ax.plot(processos, real, marker="s", linewidth=2.5,
            color=CORES[1], markersize=8, label="Speedup real")
    for x, y in zip(processos, real):
        ax.annotate(f"{y:.2f}x", (x, y),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_speedup.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_eficiencia(processos, tempos, pasta):
    t1         = tempos[0]
    eficiencia = [(t1 / t) / p * 100 for t, p in zip(tempos, processos)]

    fig, ax = _fig_base(
        "Eficiência Paralela × Número de Processos",
        "Número de Processos",
        "Eficiência (%)",
    )
    barras = ax.bar(processos, eficiencia,
                    color=CORES[2], edgecolor="white", width=0.6)
    ax.axhline(100, linestyle="--", color="gray", linewidth=1.2,
               label="Eficiência ideal (100%)")
    ax.set_ylim(0, 120)
    for bar, val in zip(barras, eficiencia):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 2,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_eficiencia.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_barras_tempo(processos, tempos, pasta):
    """Gráfico de barras comparativo do tempo de cada configuração."""
    fig, ax = _fig_base(
        "Comparativo de Tempo por Configuração",
        "Número de Processos",
        "Tempo (segundos)",
    )
    cores_barras = CORES[:len(processos)]
    barras = ax.bar([str(p) for p in processos], tempos,
                    color=cores_barras, edgecolor="white", width=0.55)
    for bar, val in zip(barras, tempos):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.01,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=9)
    ax.set_xlabel("Número de Processos")
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_barras_tempo.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark paralelo do NYC Yellow Taxi Trip Data"
    )
    parser.add_argument(
        "--max-processos", "-m",
        type=int,
        default=min(mp.cpu_count(), 8),
        help="Máximo de processos a testar (padrão: núcleos da máquina, até 8)",
    )
    args = parser.parse_args()

    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Gera lista de processos: 1, 2, 4, 8 … até max
    max_p = args.max_processos
    processos_list = sorted(
        set([1] + [2**i for i in range(1, int(log2(max_p)) + 1) if 2**i <= max_p])
    )
    if max_p not in processos_list:
        processos_list.append(max_p)

    print("=" * 60)
    print("  NYC Yellow Taxi  —  BENCHMARK")
    print(f"  Configurações: {processos_list} processos")
    print(f"  Repetições por config.: {REPETICOES}")
    print("=" * 60)

    # Lê o CSV uma única vez (não entra no tempo de processamento paralelo)
    print("\n  Lendo CSV... ", end="", flush=True)
    t_csv0 = time.perf_counter()
    linhas = ler_csv(csv_path)
    t_csv1 = time.perf_counter()
    print(f"OK  ({len(linhas):,} linhas, {t_csv1 - t_csv0:.2f}s)")

    resultados = {}   # {num_processos: tempo_médio}

    for n in processos_list:
        tempos_exec = []
        print(f"\n  Testando {n:2d} processo(s)...", end=" ", flush=True)
        for rep in range(REPETICOES):
            t = executar(linhas, n)
            tempos_exec.append(t)
            print(f"[rep {rep+1}: {t:.3f}s]", end=" ", flush=True)

        media = sum(tempos_exec) / REPETICOES
        resultados[n] = round(media, 6)
        print(f"  → média: {media:.4f}s")

    # ── Exibe tabela de resultados ──
    t_seq = resultados[1]
    print("\n" + "=" * 60)
    print(f"  {'Processos':>10}  {'Tempo (s)':>12}  {'Speedup':>10}  {'Eficiência':>12}")
    print("  " + "-" * 56)
    for n in processos_list:
        t     = resultados[n]
        sp    = t_seq / t
        ef    = sp / n * 100
        print(f"  {n:>10}  {t:>12.4f}  {sp:>10.3f}x  {ef:>11.1f}%")
    print("=" * 60)

    # ── Salva JSON de benchmark ──
    bench_data = {
        "processos":  processos_list,
        "tempos":     [resultados[n] for n in processos_list],
        "speedups":   [round(t_seq / resultados[n], 4) for n in processos_list],
        "eficiencias":[round((t_seq / resultados[n]) / n * 100, 2) for n in processos_list],
        "repeticoes": REPETICOES,
    }
    json_path = os.path.join(OUTPUT_DIR, "benchmark_dados.json")
    with open(json_path, "w") as f:
        json.dump(bench_data, f, indent=2)
    print(f"\n  Dados salvos em: {json_path}")

    # ── Gera gráficos ──
    ps  = processos_list
    ts  = [resultados[n] for n in ps]

    print(f"\n  Gerando gráficos em '{OUTPUT_DIR}/'...")
    grafico_tempo(ps, ts, OUTPUT_DIR)
    grafico_speedup(ps, ts, OUTPUT_DIR)
    grafico_eficiencia(ps, ts, OUTPUT_DIR)
    grafico_barras_tempo(ps, ts, OUTPUT_DIR)

    print(f"\n  ✅ Benchmark concluído! Gráficos em ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
