"""
=============================================================
  NYC Yellow Taxi Trip Data - BENCHMARK + GRÁFICOS
  Programação Concorrente e Distribuída
=============================================================
  ESTRATÉGIA DE MEMÓRIA E I/O (versão corrigida):

  PROBLEMA ANTERIOR:
    - Workers usavam  "for i, row: if i < linha_inicio: continue"
    - Cada worker lia o arquivo inteiro do byte 0 e descartava
      as linhas até chegar no seu intervalo — O(n) de I/O por worker.
    - Com 12 workers simultâneos: 12 leituras do CSV de 2 GB ao mesmo
      tempo → contenção de disco → speedup saturava em ~2.6×.

  CORREÇÃO:
    - Workers usam  f.seek(byte_inicio)  — saltam direto para seu bloco.
    - Cada worker lê APENAS os bytes do seu intervalo.
    - Zero contenção desnecessária de disco.

  LIMITE DE MEMÓRIA POR CORE (requisito do professor):
    - 1 core  →  500 MB máx  (passo ≤ 500 MB)
    - 2 cores →  1 GB  total  (passo ≤ 500 MB cada)
    - 4 cores →  2 GB  total
    - 8 cores →  4 GB  total
    - 12 cores →  6 GB  total
    - Valores SEMPRE CRESCENTES: mais cores = mais memória total.
    - O passo nunca excede a cota individual do core.

  Gráficos gerados:
    1. Tempo de execução × número de processos
    2. Speedup real vs. ideal
    3. Eficiência paralela
    4. Comparativo de tempo em barras
    5. Distribuição de corridas por faixa
    6. Estatísticas das distâncias (box-plot sintético)
    7. Limite de memória por configuração  ← NOVO (demonstra requisito)

  Uso:
    python taxi_benchmark.py
    python taxi_benchmark.py --max-processos 12
    python taxi_benchmark.py --max-processos 12 --chunks-por-processo 4
    python taxi_benchmark.py --max-processos 12 --carga-cpu 20
=============================================================
"""

import csv
import io
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

# Limite de memória por core: 500 MB (cresce com o número de cores)
MEM_BASE_POR_CORE = 500 * 1024 * 1024

# Histograma para percentis aproximados sem armazenar todas as distâncias.
HIST_BIN_WIDTH = 0.01
HIST_MAX       = 100.0
HIST_BINS      = int(HIST_MAX / HIST_BIN_WIDTH) + 1

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


# ══════════════════════════════════════════════════════════
#  Controle de memória e byte-chunking
# ══════════════════════════════════════════════════════════

def calcular_limite_memoria(num_processos: int) -> int:
    """Limite total (bytes). Sempre crescente: 500 MB × num_processos."""
    return MEM_BASE_POR_CORE * num_processos

def calcular_passo(tamanho_arquivo: int, num_processos: int) -> int:
    """
    Passo em bytes por processo.
    Nunca excede MEM_BASE_POR_CORE (cota individual do core).
    """
    passo_ideal = ceil(tamanho_arquivo / num_processos)
    return min(passo_ideal, MEM_BASE_POR_CORE)

def ler_cabecalho(csv_path: str) -> list:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))

def avancar_para_linha(f, offset: int) -> int:
    """Avança até a próxima linha completa após offset."""
    if offset == 0:
        return 0
    f.seek(offset)
    f.readline()
    return f.tell()

def gerar_intervalos(csv_path: str, num_processos: int,
                     chunks_por_processo: int) -> list:
    """
    Divide o arquivo em intervalos de bytes.
    Usa chunks_por_processo para melhor balanceamento de carga.
    O passo nunca excede MEM_BASE_POR_CORE.
    """
    tamanho      = os.path.getsize(csv_path)
    total_chunks = max(1, num_processos * chunks_por_processo)
    # passo baseado em chunks, mas limitado pela cota do core
    passo_chunks = ceil(tamanho / total_chunks)
    passo        = min(passo_chunks, MEM_BASE_POR_CORE)

    intervalos = []
    with open(csv_path, "rb") as f:
        f.readline()          # pula cabeçalho
        offset = f.tell()

    while offset < tamanho:
        fim = min(offset + passo, tamanho)
        intervalos.append((offset, fim))
        offset = fim

    return intervalos


# ══════════════════════════════════════════════════════════
#  Funções de cálculo
# ══════════════════════════════════════════════════════════

def classificar_faixa(dist: float) -> str | None:
    for baixo, alto, label in FAIXAS:
        if baixo < dist <= alto:
            return label
    return None

def hist_index(dist: float) -> int:
    if dist < 0:
        return 0
    if dist >= HIST_MAX:
        return HIST_BINS - 1
    return int(dist / HIST_BIN_WIDTH)

def carga_cpu_controlada(dist: float, repeticoes: int) -> float:
    """Carga computacional opcional para tornar o benchmark mais CPU-bound."""
    valor = dist
    for _ in range(repeticoes):
        valor = math.sqrt(valor * valor + 1.000001)
    return valor


# ══════════════════════════════════════════════════════════
#  Worker — salta direto para o bloco de bytes com f.seek()
# ══════════════════════════════════════════════════════════

def processar_intervalo(args: tuple) -> dict:
    """
    MAP — salta com f.seek(byte_inicio) e lê SOMENTE seu bloco.
    Não percorre o arquivo do byte 0.
    """
    csv_path, byte_inicio, byte_fim, colunas, carga_cpu = args

    soma           = 0.0
    soma_quadrados = 0.0
    contagem       = 0
    maior          = float("-inf")
    menor          = float("inf")
    hist           = [0] * HIST_BINS
    distribuicao   = {label: 0 for _, _, label in FAIXAS}

    with open(csv_path, "rb") as f_raw:
        inicio_real = avancar_para_linha(f_raw, byte_inicio)
        tamanho     = byte_fim - inicio_real
        if tamanho <= 0:
            return {"soma": 0.0, "soma_quadrados": 0.0, "contagem": 0,
                    "maior": 0.0, "menor": 0.0,
                    "hist": hist, "distribuicao": distribuicao}
        bloco = f_raw.read(tamanho)   # lê SOMENTE seu pedaço

    reader = csv.DictReader(
        io.StringIO(bloco.decode("utf-8", errors="replace")),
        fieldnames=colunas,
    )
    for row in reader:
        try:
            dist = float(row[DIST_COL])
        except (ValueError, KeyError, TypeError):
            continue
        if dist <= 0:
            continue

        _ = carga_cpu_controlada(dist, carga_cpu)

        soma           += dist
        soma_quadrados += dist * dist
        contagem       += 1

        if dist > maior: maior = dist
        if dist < menor: menor = dist

        hist[hist_index(dist)] += 1
        faixa = classificar_faixa(dist)
        if faixa:
            distribuicao[faixa] += 1

    return {
        "soma":           soma,
        "soma_quadrados": soma_quadrados,
        "contagem":       contagem,
        "maior":          maior if contagem else 0.0,
        "menor":          menor if contagem else 0.0,
        "hist":           hist,
        "distribuicao":   distribuicao,
    }


# ══════════════════════════════════════════════════════════
#  REDUCE
# ══════════════════════════════════════════════════════════

def percentil_por_histograma(hist: list, p: float, total: int) -> float:
    if total <= 0:
        return 0.0
    alvo      = (p / 100) * total
    acumulado = 0
    for idx, qtd in enumerate(hist):
        acumulado += qtd
        if acumulado >= alvo:
            return round(idx * HIST_BIN_WIDTH, 4)
    return HIST_MAX

def combinar(parciais: list) -> dict:
    soma = soma_quadrados = contagem = 0
    maior = float("-inf")
    menor = float("inf")
    hist_total = [0] * HIST_BINS
    dist_total = {label: 0 for _, _, label in FAIXAS}

    for p in parciais:
        soma           += p["soma"]
        soma_quadrados += p["soma_quadrados"]
        contagem       += p["contagem"]
        if p["maior"] > maior: maior = p["maior"]
        if p["menor"] < menor: menor = p["menor"]
        for i, qtd in enumerate(p["hist"]):
            hist_total[i] += qtd
        for faixa, qtd in p["distribuicao"].items():
            dist_total[faixa] += qtd

    media    = soma / contagem if contagem else 0.0
    varianca = (soma_quadrados / contagem - media * media) if contagem else 0.0
    desvio   = math.sqrt(max(varianca, 0.0))

    return {
        "soma_total":     round(soma,  4),
        "media":          round(media, 4),
        "maior_corrida":  round(maior if contagem else 0.0, 4),
        "menor_corrida":  round(menor if contagem else 0.0, 4),
        "total_corridas": contagem,
        "desvio_padrao":  round(desvio, 4),
        "mediana":        percentil_por_histograma(hist_total, 50, contagem),
        "percentil_25":   percentil_por_histograma(hist_total, 25, contagem),
        "percentil_75":   percentil_por_histograma(hist_total, 75, contagem),
        "percentil_90":   percentil_por_histograma(hist_total, 90, contagem),
        "percentil_99":   percentil_por_histograma(hist_total, 99, contagem),
        "distribuicao":   dist_total,
    }


# ══════════════════════════════════════════════════════════
#  Execução de uma configuração
# ══════════════════════════════════════════════════════════

def executar(csv_path: str, colunas: list, num_processos: int,
             chunks_por_processo: int, carga_cpu: int):
    intervalos = gerar_intervalos(csv_path, num_processos, chunks_por_processo)
    args = [(csv_path, ini, fim, colunas, carga_cpu)
            for ini, fim in intervalos]

    t0 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(processar_intervalo, args)
    resultado = combinar(parciais)
    return time.perf_counter() - t0, resultado, len(intervalos)


# ══════════════════════════════════════════════════════════
#  Gráficos
# ══════════════════════════════════════════════════════════

def _fig_base(titulo, xlabel, ylabel):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title(titulo, fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    return fig, ax

def _salvar(fig, pasta, nome):
    path = os.path.join(pasta, nome)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")

def grafico_tempo(processos, tempos, pasta):
    fig, ax = _fig_base("Tempo de Execução × Número de Processos",
                        "Número de Processos", "Tempo (segundos)")
    ax.plot(processos, tempos, marker="o", linewidth=2.5,
            color=CORES[0], markersize=8, label="Tempo medido")
    for x, y in zip(processos, tempos):
        ax.annotate(f"{y:.2f}s", (x, y),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    _salvar(fig, pasta, "grafico_tempo.png")

def grafico_speedup(processos, tempos, pasta):
    t1   = tempos[0]
    real = [t1 / t for t in tempos]
    fig, ax = _fig_base("Speedup × Número de Processos",
                        "Número de Processos", "Speedup")
    ax.plot(processos, processos, linestyle="--", linewidth=1.5,
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
    _salvar(fig, pasta, "grafico_speedup.png")

def grafico_eficiencia(processos, tempos, pasta):
    t1         = tempos[0]
    eficiencia = [(t1 / t) / p * 100 for t, p in zip(tempos, processos)]
    fig, ax = _fig_base("Eficiência Paralela × Número de Processos",
                        "Número de Processos", "Eficiência (%)")
    barras = ax.bar(processos, eficiencia,
                    color=CORES[2], edgecolor="white", width=0.6)
    ax.axhline(100, linestyle="--", color="gray",
               linewidth=1.2, label="Eficiência ideal (100%)")
    ax.set_ylim(0, max(120, max(eficiencia) * 1.15))
    for bar, val in zip(barras, eficiencia):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 2, f"{val:.1f}%",
                ha="center", va="bottom", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    _salvar(fig, pasta, "grafico_eficiencia.png")

def grafico_barras_tempo(processos, tempos, pasta):
    fig, ax = _fig_base("Comparativo de Tempo por Configuração",
                        "Número de Processos", "Tempo (segundos)")
    barras = ax.bar([str(p) for p in processos], tempos,
                    color=CORES[:len(processos)], edgecolor="white", width=0.55)
    for bar, val in zip(barras, tempos):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.01, f"{val:.2f}s",
                ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    _salvar(fig, pasta, "grafico_barras_tempo.png")

def grafico_memoria(processos, tamanho_arquivo, pasta):
    """
    Demonstra o requisito do professor:
      - Limite total SEMPRE CRESCENTE com mais cores.
      - Passo por processo NUNCA excede a cota individual (500 MB).
    """
    limites_mb = [calcular_limite_memoria(p) / 1024**2 for p in processos]
    passos_mb  = [calcular_passo(tamanho_arquivo, p) / 1024**2 for p in processos]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Controle de Memória por Configuração de Cores",
                 fontsize=14, fontweight="bold")

    # Limite total crescente
    barras = ax1.bar([str(p) for p in processos], limites_mb,
                     color=CORES[3], edgecolor="white", width=0.55)
    ax1.plot(range(len(processos)), limites_mb, marker="o",
             color="gray", linewidth=1.5, linestyle="--",
             label=f"{MEM_BASE_POR_CORE // 1024**2} MB × cores")
    for bar, val in zip(barras, limites_mb):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + max(limites_mb) * 0.01,
                 f"{val:.0f} MB", ha="center", va="bottom",
                 fontsize=9, fontweight="bold")
    ax1.set_title("Limite Total de Memória\n(cresce linearmente com os cores)",
                  fontsize=11)
    ax1.set_xlabel("Número de Cores", fontsize=11)
    ax1.set_ylabel("Memória Máxima Total (MB)", fontsize=11)
    ax1.set_ylim(0, max(limites_mb) * 1.2)
    ax1.set_xticks(range(len(processos)))
    ax1.set_xticklabels([str(p) for p in processos])
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend()

    # Passo por processo — nunca excede 500 MB
    barras2 = ax2.bar([str(p) for p in processos], passos_mb,
                      color=CORES[1], edgecolor="white", width=0.55)
    ax2.axhline(MEM_BASE_POR_CORE / 1024**2, linestyle="--",
                color="red", linewidth=1.8,
                label=f"Limite máx por core ({MEM_BASE_POR_CORE // 1024**2} MB)")
    for bar, val in zip(barras2, passos_mb):
        ax2.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + max(passos_mb) * 0.02,
                 f"{val:.0f} MB", ha="center", va="bottom", fontsize=9)
    ax2.set_title("Passo por Processo\n(nunca excede a cota do core)",
                  fontsize=11)
    ax2.set_xlabel("Número de Cores", fontsize=11)
    ax2.set_ylabel("Bytes lidos por processo (MB)", fontsize=11)
    ax2.set_ylim(0, MEM_BASE_POR_CORE / 1024**2 * 1.3)
    ax2.set_xticks(range(len(processos)))
    ax2.set_xticklabels([str(p) for p in processos])
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend()

    fig.tight_layout()
    _salvar(fig, pasta, "grafico_memoria.png")

def grafico_distribuicao(distribuicao: dict, total_corridas: int, pasta: str):
    labels  = list(distribuicao.keys())
    valores = list(distribuicao.values())
    pcts    = [v / total_corridas * 100 if total_corridas > 0 else 0
               for v in valores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribuição de Corridas por Faixa de Distância",
                 fontsize=14, fontweight="bold")

    ax1.pie(valores, labels=labels, colors=CORES[:len(labels)],
            autopct="%1.1f%%", startangle=140, pctdistance=0.82)
    ax1.set_title("Proporção (%)", fontsize=11)

    barras = ax2.barh(labels, pcts, color=CORES[:len(labels)], edgecolor="white")
    ax2.set_xlabel("% das corridas", fontsize=11)
    ax2.set_title("Distribuição (%)", fontsize=11)
    ax2.set_xlim(0, max(pcts) * 1.18 if pcts else 1)
    for bar, pct, qtd in zip(barras, pcts, valores):
        ax2.text(bar.get_width() + 0.3,
                 bar.get_y() + bar.get_height() / 2,
                 f"{pct:.1f}%  ({qtd:,})", va="center", fontsize=9)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.4)
    fig.tight_layout()
    _salvar(fig, pasta, "grafico_distribuicao.png")

def grafico_estatisticas(resultado: dict, pasta: str):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title("Resumo Estatístico das Distâncias (milhas)",
                 fontsize=14, fontweight="bold", pad=12)

    p25 = resultado["percentil_25"]
    med = resultado["mediana"]
    p75 = resultado["percentil_75"]
    p90 = resultado["percentil_90"]

    ax.barh(0, p75 - p25, left=p25, height=0.4,
            color=CORES[0], alpha=0.7, label="IQR (P25–P75)")
    ax.vlines(med, -0.2, 0.2, color="white", linewidth=3, zorder=5)
    ax.vlines(med, -0.2, 0.2, color=CORES[1], linewidth=2,
              zorder=6, label=f"Mediana ≈ {med:.2f} mi")
    ax.hlines(0, resultado["menor_corrida"], p25, color=CORES[0], linewidth=2)
    ax.hlines(0, p75, p90, color=CORES[0], linewidth=2)
    ax.vlines([resultado["menor_corrida"], p90], -0.12, 0.12,
              color=CORES[0], linewidth=2)

    for valor, label, offset in [
        (resultado["menor_corrida"], "Mín",   -0.32),
        (p25,                        "P25",   -0.32),
        (med,                        "P50",    0.32),
        (p75,                        "P75",   -0.32),
        (p90,                        "P90",    0.32),
        (resultado["media"],          "Média",  0.52),
    ]:
        ax.annotate(f"{label}\n{valor:.2f}", xy=(valor, 0),
                    xytext=(valor, offset), ha="center", fontsize=8,
                    arrowprops=dict(arrowstyle="-", color="gray", lw=0.8))

    ax.set_yticks([])
    ax.set_xlabel("Distância (milhas)", fontsize=12)
    ax.set_xlim(resultado["menor_corrida"] * 0.8,
                max(resultado["percentil_99"] * 1.5, 1))
    ax.grid(True, axis="x", linestyle="--", alpha=0.4)
    ax.legend(loc="upper right")
    fig.tight_layout()
    _salvar(fig, pasta, "grafico_estatisticas.png")


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark paralelo do NYC Yellow Taxi (byte-seek, memória controlada)"
    )
    parser.add_argument("--max-processos", "-m", type=int,
                        default=min(mp.cpu_count(), 12),
                        help="Máximo de processos a testar")
    parser.add_argument("--repeticoes", "-r", type=int, default=REPETICOES,
                        help="Repetições por configuração")
    parser.add_argument("--chunks-por-processo", "-c", type=int, default=4,
                        help="Chunks por processo (balanceamento de carga)")
    parser.add_argument("--carga-cpu", type=int, default=20,
                        help="Cálculos extras por registro. Use 0 para desativar")
    parser.add_argument("--csv", default=CSV_FILE,
                        help="Caminho do CSV")
    args = parser.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    max_p         = max(1, args.max_processos)
    processos_set = set([1] + [2**i for i in range(1, int(log2(max(max_p, 2))) + 1)
                               if 2**i <= max_p])
    processos_set.add(max_p)
    processos_list = sorted(processos_set)

    tamanho_arquivo = os.path.getsize(csv_path)
    colunas         = ler_cabecalho(csv_path)

    print("=" * 72)
    print("  NYC Yellow Taxi  —  BENCHMARK (byte-seek + memória controlada)")
    print(f"  Configurações       : {processos_list} processos")
    print(f"  Repetições          : {args.repeticoes} por configuração")
    print(f"  Chunks por processo : {args.chunks_por_processo}")
    print(f"  Carga CPU           : {args.carga_cpu}")
    print(f"  Arquivo             : {tamanho_arquivo / 1024**2:.1f} MB")
    print("=" * 72)

    print("\n  Tabela de memória por configuração:")
    print(f"  {'Cores':>6}  {'Limite total':>14}  {'Passo/processo':>16}")
    print("  " + "-" * 40)
    for n in processos_list:
        lim   = calcular_limite_memoria(n) / 1024**2
        passo = calcular_passo(tamanho_arquivo, n) / 1024**2
        print(f"  {n:>6}  {lim:>12.0f} MB  {passo:>14.1f} MB")
    print()

    resultados_tempo  = {}
    chunks_por_config = {}
    resultado_final   = None

    for n in processos_list:
        print(f"\n  Aquecimento com {n:2d} processo(s)...", end=" ", flush=True)
        _, _, qtd = executar(csv_path, colunas, n,
                             args.chunks_por_processo, args.carga_cpu)
        print(f"OK ({qtd} chunks)")

        tempos_exec = []
        print(f"  Testando    com {n:2d} processo(s)...", end=" ", flush=True)
        for rep in range(args.repeticoes):
            t, res, qtd = executar(csv_path, colunas, n,
                                   args.chunks_por_processo, args.carga_cpu)
            tempos_exec.append(t)
            resultado_final   = res
            chunks_por_config[n] = qtd
            print(f"[rep {rep + 1}: {t:.3f}s]", end=" ", flush=True)

        mediana_t = sorted(tempos_exec)[len(tempos_exec) // 2]
        resultados_tempo[n] = round(mediana_t, 6)
        print(f"  → mediana: {mediana_t:.4f}s")

    t_seq = resultados_tempo[1]
    ps    = processos_list
    ts    = [resultados_tempo[n] for n in ps]

    print("\n" + "=" * 72)
    print(f"  {'Processos':>10}  {'Tempo (s)':>12}  {'Speedup':>10}  "
          f"{'Eficiência':>12}  {'Chunks':>8}")
    print("  " + "-" * 68)
    for n in ps:
        t  = resultados_tempo[n]
        sp = t_seq / t
        ef = sp / n * 100
        print(f"  {n:>10}  {t:>12.4f}  {sp:>10.3f}x  "
              f"{ef:>11.1f}%  {chunks_por_config[n]:>8}")
    print("=" * 72)

    if resultado_final:
        print(f"\n  {'─' * 58}")
        print("  MÉTRICAS DAS CORRIDAS")
        print(f"  {'─' * 58}")
        r = resultado_final
        print(f"  {'Total de corridas':<30}: {r['total_corridas']:>14,}")
        print(f"  {'Soma total (mi)':<30}: {r['soma_total']:>14,.4f}")
        print(f"  {'Média (mi)':<30}: {r['media']:>14.4f}")
        print(f"  {'Mediana aprox. (mi)':<30}: {r['mediana']:>14.4f}")
        print(f"  {'Maior corrida (mi)':<30}: {r['maior_corrida']:>14.4f}")
        print(f"  {'Menor corrida (mi)':<30}: {r['menor_corrida']:>14.4f}")
        print(f"  {'Desvio padrão (mi)':<30}: {r['desvio_padrao']:>14.4f}")

    bench_data = {
        "processos":      ps,
        "tempos":         ts,
        "speedups":       [round(t_seq / resultados_tempo[n], 4) for n in ps],
        "eficiencias":    [round((t_seq / resultados_tempo[n]) / n * 100, 2)
                          for n in ps],
        "repeticoes":     args.repeticoes,
        "chunks_por_processo": args.chunks_por_processo,
        "carga_cpu":      args.carga_cpu,
        "hist_bin_width": HIST_BIN_WIDTH,
        "hist_max":       HIST_MAX,
        "tamanho_arquivo_mb": round(tamanho_arquivo / 1024**2, 1),
        "limites_memoria_mb": [calcular_limite_memoria(n) // 1024**2 for n in ps],
        "passos_mb":      [round(calcular_passo(tamanho_arquivo, n) / 1024**2, 1)
                          for n in ps],
        "chunks_por_configuracao": {str(k): v for k, v in chunks_por_config.items()},
        "metricas_corridas": resultado_final or {},
        "observacao": (
            "Tempos = mediana das repetições. "
            "Workers usam f.seek() — leem apenas seu bloco de bytes. "
            "Percentis estimados por histograma (bin=0.01mi). "
            f"Passo limitado a {MEM_BASE_POR_CORE // 1024**2} MB/core."
        ),
    }
    json_path = os.path.join(OUTPUT_DIR, "benchmark_dados.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(bench_data, f, indent=2, ensure_ascii=False)
    print(f"\n  Dados salvos em: {json_path}")

    print(f"\n  Gerando gráficos em '{OUTPUT_DIR}/'...")
    grafico_tempo(ps, ts, OUTPUT_DIR)
    grafico_speedup(ps, ts, OUTPUT_DIR)
    grafico_eficiencia(ps, ts, OUTPUT_DIR)
    grafico_barras_tempo(ps, ts, OUTPUT_DIR)
    grafico_memoria(ps, tamanho_arquivo, OUTPUT_DIR)   # ← NOVO

    if resultado_final:
        grafico_distribuicao(resultado_final["distribuicao"],
                             resultado_final["total_corridas"], OUTPUT_DIR)
        grafico_estatisticas(resultado_final, OUTPUT_DIR)

    print(f"\n  ✅ Benchmark concluído! Gráficos em ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()