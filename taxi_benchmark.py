"""
=============================================================
  NYC Yellow Taxi Trip Data - BENCHMARK + GRÁFICOS
  Programação Concorrente e Distribuída
=============================================================
  Filtro aplicado: distâncias entre DIST_MIN e P99
  Gráficos gerados:
    1. Tempo de execução × número de processos
    2. Speedup (real vs. ideal)
    3. Eficiência paralela
    4. Comparativo de tempo em barras
    5. Distribuição de corridas por faixa
    6. Estatísticas (box-plot sintético)

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
import math
import argparse
import multiprocessing as mp
from math import ceil, log2

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ─── Configuração ─────────────────────────────────────────
CSV_FILE   = "yellow_tripdata_2015-01.csv"
DIST_COL   = "trip_distance"
OUTPUT_DIR = "graficos_benchmark"
REPETICOES = 3
DIST_MIN   = 0.1

FAIXAS = [
    (0.0,  1.0,  "0–1 mi"),
    (1.0,  3.0,  "1–3 mi"),
    (3.0,  7.0,  "3–7 mi"),
    (7.0,  15.0, "7–15 mi"),
    (15.0, float("inf"), ">15 mi"),
]

CORES = ["#2196F3", "#4CAF50", "#FF5722", "#9C27B0",
         "#FF9800", "#00BCD4", "#E91E63", "#8BC34A"]
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


# ── Filtro P99 ────────────────────────────────────────────

def calcular_limite_p99(csv_path):
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
    linhas, limite_p99 = args
    soma = contagem = removidos = 0
    maior = float("-inf")
    menor = float("inf")
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
        "soma": soma, "contagem": contagem, "removidos": removidos,
        "maior": maior if contagem > 0 else 0.0,
        "menor": menor if contagem > 0 else 0.0,
        "distancias": distancias,
    }


def combinar(parciais, limite_p99):
    soma = contagem = removidos = 0
    maior = float("-inf")
    menor = float("inf")
    todas = []
    for p in parciais:
        soma     += p["soma"]
        contagem += p["contagem"]
        removidos += p["removidos"]
        todas    += p["distancias"]
        if p["maior"] > maior: maior = p["maior"]
        if p["menor"] < menor: menor = p["menor"]
    media = calcular_media(todas)
    todas.sort()
    return {
        "soma_total":         round(soma, 4),
        "media":              round(media, 4),
        "maior_corrida":      round(maior, 4),
        "menor_corrida":      round(menor, 4),
        "total_corridas":     contagem,
        "outliers_removidos": removidos,
        "limite_p99":         round(limite_p99, 4),
        "desvio_padrao":      round(calcular_desvio_padrao(todas, media), 4),
        "mediana":            round(calcular_percentil(todas, 50), 4),
        "percentil_25":       round(calcular_percentil(todas, 25), 4),
        "percentil_75":       round(calcular_percentil(todas, 75), 4),
        "percentil_90":       round(calcular_percentil(todas, 90), 4),
        "distribuicao":       calcular_distribuicao(todas),
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

def executar(linhas_cache, num_processos, limite_p99):
    chunks = dividir(linhas_cache, num_processos)
    args   = [(chunk, limite_p99) for chunk in chunks]
    t0 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(processar_chunk, args)
    resultado = combinar(parciais, limite_p99)
    return time.perf_counter() - t0, resultado


# ── Gráficos ──────────────────────────────────────────────

def _fig_base(titulo, xlabel, ylabel):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(titulo, fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    return fig, ax

def grafico_tempo(ps, ts, pasta):
    fig, ax = _fig_base("Tempo de Execução × Número de Processos",
                        "Número de Processos", "Tempo (segundos)")
    ax.plot(ps, ts, marker="o", linewidth=2.5, color=CORES[0], markersize=8)
    for x, y in zip(ps, ts):
        ax.annotate(f"{y:.2f}s", (x, y), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xticks(ps)
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_tempo.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")

def grafico_speedup(ps, ts, pasta):
    t1   = ts[0]
    real = [t1 / t for t in ts]
    fig, ax = _fig_base("Speedup × Número de Processos",
                        "Número de Processos", "Speedup")
    ax.plot(ps, ps, linestyle="--", linewidth=1.5, color="gray", label="Ideal")
    ax.plot(ps, real, marker="s", linewidth=2.5, color=CORES[1],
            markersize=8, label="Real")
    for x, y in zip(ps, real):
        ax.annotate(f"{y:.2f}x", (x, y), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xticks(ps)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_speedup.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")

def grafico_eficiencia(ps, ts, pasta):
    t1  = ts[0]
    efi = [(t1 / t) / p * 100 for t, p in zip(ts, ps)]
    fig, ax = _fig_base("Eficiência Paralela × Número de Processos",
                        "Número de Processos", "Eficiência (%)")
    barras = ax.bar(ps, efi, color=CORES[2], edgecolor="white", width=0.6)
    ax.axhline(100, linestyle="--", color="gray", linewidth=1.2, label="Ideal (100%)")
    ax.set_ylim(0, 120)
    for bar, val in zip(barras, efi):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(ps)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_eficiencia.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")

def grafico_barras_tempo(ps, ts, pasta):
    fig, ax = _fig_base("Comparativo de Tempo por Configuração",
                        "Número de Processos", "Tempo (segundos)")
    barras = ax.bar([str(p) for p in ps], ts,
                    color=CORES[:len(ps)], edgecolor="white", width=0.55)
    for bar, val in zip(barras, ts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_barras_tempo.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")

def grafico_distribuicao(distribuicao, total, pasta):
    labels  = list(distribuicao.keys())
    valores = list(distribuicao.values())
    pcts    = [v / total * 100 if total > 0 else 0 for v in valores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribuição de Corridas por Faixa de Distância (pós-filtro P99)",
                 fontsize=13, fontweight="bold")
    ax1.pie(valores, labels=labels, colors=CORES[:len(labels)],
            autopct="%1.1f%%", startangle=140, pctdistance=0.82)
    ax1.set_title("Proporção (%)")
    barras = ax2.barh(labels, pcts, color=CORES[:len(labels)], edgecolor="white")
    ax2.set_xlabel("% das corridas")
    ax2.set_xlim(0, max(pcts) * 1.18)
    for bar, pct, qtd in zip(barras, pcts, valores):
        ax2.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                 f"{pct:.1f}%  ({qtd:,})", va="center", fontsize=9)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_distribuicao.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")

def grafico_estatisticas(resultado, pasta):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title("Resumo Estatístico das Distâncias — pós-filtro P99",
                 fontsize=13, fontweight="bold", pad=12)
    p25 = resultado["percentil_25"]
    med = resultado["mediana"]
    p75 = resultado["percentil_75"]
    p90 = resultado["percentil_90"]
    p10 = p25 * 0.6

    ax.barh(0, p75 - p25, left=p25, height=0.4,
            color=CORES[0], alpha=0.7, label="IQR (P25–P75)")
    ax.vlines(med, -0.2, 0.2, color="white", linewidth=3, zorder=5)
    ax.vlines(med, -0.2, 0.2, color=CORES[1], linewidth=2,
              zorder=6, label=f"Mediana = {med:.2f} mi")
    ax.hlines(0, p10, p25, color=CORES[0], linewidth=2)
    ax.hlines(0, p75, p90, color=CORES[0], linewidth=2)
    ax.vlines([p10, p90], -0.12, 0.12, color=CORES[0], linewidth=2)

    for valor, label, offset in [
        (resultado["menor_corrida"], "Mín",   -0.32),
        (p25,                        "P25",   -0.32),
        (med,                        "P50",    0.32),
        (p75,                        "P75",   -0.32),
        (p90,                        "P90",    0.32),
        (resultado["maior_corrida"], "Máx",    0.32),
        (resultado["media"],         "Média",  0.52),
    ]:
        ax.annotate(f"{label}\n{valor:.2f}", xy=(valor, 0), xytext=(valor, offset),
                    ha="center", fontsize=8,
                    arrowprops=dict(arrowstyle="-", color="gray", lw=0.8))

    ax.set_yticks([])
    ax.set_xlabel("Distância (milhas)", fontsize=12)
    ax.set_xlim(resultado["menor_corrida"] * 0.8, resultado["maior_corrida"] * 1.1)
    ax.grid(True, axis="x", linestyle="--", alpha=0.4)
    ax.legend(loc="upper right")
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_estatisticas.png")
    fig.savefig(path, dpi=150); plt.close(fig)
    print(f"  [✓] {path}")


# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-processos", "-m", type=int,
                        default=min(mp.cpu_count(), 8))
    args = parser.parse_args()

    csv_path = CSV_FILE
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    max_p = args.max_processos
    ps = sorted(set([1] + [2**i for i in range(1, int(log2(max(max_p, 2))) + 1)
                            if 2**i <= max_p]))
    if max_p not in ps:
        ps.append(max_p)

    print("=" * 62)
    print("  NYC Yellow Taxi  —  BENCHMARK")
    print(f"  Configurações : {ps} processos  |  Filtro: P99")
    print(f"  Repetições    : {REPETICOES} por configuração")
    print("=" * 62)

    # Calcula P99 uma única vez
    print("\n  Calculando P99...", end=" ", flush=True)
    limite_p99 = calcular_limite_p99(csv_path)
    print(f"limite = {limite_p99:.4f} milhas")

    # Lê CSV uma única vez
    print("  Lendo CSV...", end=" ", flush=True)
    linhas = ler_csv(csv_path)
    print(f"OK  ({len(linhas):,} linhas)")

    resultados_tempo = {}
    resultado_final  = None

    for n in ps:
        tempos_exec = []
        print(f"\n  Testando {n:2d} processo(s)...", end=" ", flush=True)
        for rep in range(REPETICOES):
            t, res = executar(linhas, n, limite_p99)
            tempos_exec.append(t)
            if resultado_final is None:
                resultado_final = res
            print(f"[rep {rep+1}: {t:.3f}s]", end=" ", flush=True)
        media_t = sum(tempos_exec) / REPETICOES
        resultados_tempo[n] = round(media_t, 6)
        print(f"  → média: {media_t:.4f}s")

    # Tabela
    t_seq = resultados_tempo[1]
    print("\n" + "=" * 62)
    print(f"  {'Processos':>10}  {'Tempo (s)':>12}  {'Speedup':>10}  {'Eficiência':>12}")
    print("  " + "-" * 58)
    for n in ps:
        t  = resultados_tempo[n]
        sp = t_seq / t
        ef = sp / n * 100
        print(f"  {n:>10}  {t:>12.4f}  {sp:>10.3f}x  {ef:>11.1f}%")
    print("=" * 62)

    if resultado_final:
        print(f"\n  Filtro aplicado  : {DIST_MIN} mi – {resultado_final['limite_p99']} mi (P99)")
        print(f"  Outliers removidos: {resultado_final['outliers_removidos']:,}")
        print(f"  Corridas válidas  : {resultado_final['total_corridas']:,}")
        print(f"  Soma total        : {resultado_final['soma_total']:,.4f} milhas")
        print(f"  Média             : {resultado_final['media']:.4f} milhas")
        print(f"  Maior corrida     : {resultado_final['maior_corrida']:.4f} milhas")
        print(f"  Desvio padrão     : {resultado_final['desvio_padrao']:.4f} milhas")

    # JSON
    ts_list = [resultados_tempo[n] for n in ps]
    bench_data = {
        "processos":   ps,
        "tempos":      ts_list,
        "speedups":    [round(t_seq / resultados_tempo[n], 4) for n in ps],
        "eficiencias": [round((t_seq / resultados_tempo[n]) / n * 100, 2) for n in ps],
        "repeticoes":  REPETICOES,
        "filtro":      {"dist_min": DIST_MIN, "limite_p99": limite_p99},
        "metricas":    {k: v for k, v in (resultado_final or {}).items()
                        if k != "distancias"},
    }
    json_path = os.path.join(OUTPUT_DIR, "benchmark_dados.json")
    with open(json_path, "w") as f:
        json.dump(bench_data, f, indent=2, ensure_ascii=False)
    print(f"\n  Dados salvos em: {json_path}")

    # Gráficos
    print(f"\n  Gerando gráficos em '{OUTPUT_DIR}/'...")
    grafico_tempo(ps, ts_list, OUTPUT_DIR)
    grafico_speedup(ps, ts_list, OUTPUT_DIR)
    grafico_eficiencia(ps, ts_list, OUTPUT_DIR)
    grafico_barras_tempo(ps, ts_list, OUTPUT_DIR)
    if resultado_final:
        grafico_distribuicao(resultado_final["distribuicao"],
                             resultado_final["total_corridas"], OUTPUT_DIR)
        grafico_estatisticas(resultado_final, OUTPUT_DIR)

    print(f"\n  ✅ Benchmark concluído! Gráficos em ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
