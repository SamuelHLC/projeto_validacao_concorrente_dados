"""
=============================================================
  NYC Yellow Taxi Trip Data - BENCHMARK + GRÁFICOS
  Programação Concorrente e Distribuída
=============================================================

  Versão ajustada para benchmark mais estável e com menor uso de memória.

  Principais ajustes:
    1. O CSV NÃO é carregado inteiro em memória.
    2. Os workers recebem apenas intervalos de linhas.
    3. Cada worker abre o CSV e processa seu próprio intervalo.
    4. O processamento usa Map-Reduce com retorno compacto.
    5. Percentis são estimados por histograma, evitando guardar milhões
       de distâncias em listas gigantes.
    6. O número de chunks pode ser maior que o número de processos,
       melhorando o balanceamento.
    7. A carga de CPU pode ser ajustada para evidenciar melhor o ganho
       paralelo em tarefas CPU-bound.

  Uso:
    python taxi_benchmark.py
    python taxi_benchmark.py --max-processos 12
    python taxi_benchmark.py --max-processos 12 --chunks-por-processo 4
    python taxi_benchmark.py --max-processos 12 --carga-cpu 20
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

# Histograma usado para percentis aproximados sem armazenar todas as distâncias.
# 0.01 milha ≈ 16 metros. É uma precisão suficiente para análise estatística.
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


def contar_linhas_csv(csv_path: str) -> int:
    """Conta as linhas de dados do CSV sem carregar o arquivo em memória."""
    with open(csv_path, "rb") as f:
        return max(sum(1 for _ in f) - 1, 0)


def montar_intervalos(total_linhas: int, num_processos: int, chunks_por_processo: int) -> list[tuple[int, int]]:
    """
    Divide o arquivo em mais chunks do que processos.

    Exemplo: 12 processos e 4 chunks por processo = 48 chunks.
    Isso melhora o balanceamento: quando um worker termina um pedaço,
    ele já pega outro, reduzindo tempo ocioso.
    """
    total_chunks = max(1, num_processos * chunks_por_processo)
    tamanho = ceil(total_linhas / total_chunks)
    return [
        (inicio, min(inicio + tamanho, total_linhas))
        for inicio in range(0, total_linhas, tamanho)
    ]


def classificar_faixa(dist: float) -> str | None:
    for baixo, alto, label in FAIXAS:
        if baixo < dist <= alto:
            return label
    return None


def hist_index(dist: float) -> int:
    """Converte uma distância em índice de histograma."""
    if dist < 0:
        return 0
    if dist >= HIST_MAX:
        return HIST_BINS - 1
    return int(dist / HIST_BIN_WIDTH)


def carga_cpu_controlada(dist: float, repeticoes: int) -> float:
    """
    Carga computacional opcional e determinística.

    Ela simula uma análise estatística mais pesada sobre cada registro,
    tornando o benchmark mais CPU-bound. Isso reduz a influência de I/O,
    criação de processos e overhead fixo, permitindo observar melhor o
    ganho real do paralelismo.

    Use --carga-cpu 0 para desativar.
    """
    valor = dist
    for _ in range(repeticoes):
        valor = math.sqrt(valor * valor + 1.000001)
    return valor


def processar_intervalo(args: tuple) -> dict:
    """
    MAP — cada worker processa um intervalo de linhas.

    O worker recebe apenas:
      - caminho do CSV;
      - linha inicial;
      - linha final;
      - carga de CPU configurada.

    Ele NÃO recebe listas de linhas, evitando pickle de grandes blocos de dados.
    """
    csv_path, linha_inicio, linha_fim, carga_cpu = args

    soma = 0.0
    soma_quadrados = 0.0
    contagem = 0
    maior = float("-inf")
    menor = float("inf")
    hist = [0] * HIST_BINS
    distribuicao = {label: 0 for _, _, label in FAIXAS}

    with open(csv_path, newline="", encoding="utf-8") as f:
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

            # Mantém o valor original para as métricas finais.
            # A carga computacional é usada apenas para simular análise pesada.
            _ = carga_cpu_controlada(dist, carga_cpu)

            soma += dist
            soma_quadrados += dist * dist
            contagem += 1

            if dist > maior:
                maior = dist
            if dist < menor:
                menor = dist

            hist[hist_index(dist)] += 1
            faixa = classificar_faixa(dist)
            if faixa:
                distribuicao[faixa] += 1

    return {
        "soma": soma,
        "soma_quadrados": soma_quadrados,
        "contagem": contagem,
        "maior": maior if contagem else 0.0,
        "menor": menor if contagem else 0.0,
        "hist": hist,
        "distribuicao": distribuicao,
    }


def percentil_por_histograma(hist: list[int], p: float, total: int) -> float:
    """Calcula percentil aproximado a partir do histograma agregado."""
    if total <= 0:
        return 0.0

    alvo = (p / 100) * total
    acumulado = 0

    for idx, qtd in enumerate(hist):
        acumulado += qtd
        if acumulado >= alvo:
            return round(idx * HIST_BIN_WIDTH, 4)

    return HIST_MAX


def combinar(parciais: list[dict]) -> dict:
    """REDUCE — combina resultados parciais compactos."""
    soma = 0.0
    soma_quadrados = 0.0
    contagem = 0
    maior = float("-inf")
    menor = float("inf")
    hist_total = [0] * HIST_BINS
    dist_total = {label: 0 for _, _, label in FAIXAS}

    for p in parciais:
        soma += p["soma"]
        soma_quadrados += p["soma_quadrados"]
        contagem += p["contagem"]

        if p["maior"] > maior:
            maior = p["maior"]
        if p["menor"] < menor:
            menor = p["menor"]

        for i, qtd in enumerate(p["hist"]):
            hist_total[i] += qtd

        for faixa, qtd in p["distribuicao"].items():
            dist_total[faixa] += qtd

    media = soma / contagem if contagem else 0.0
    variancia = (soma_quadrados / contagem - media * media) if contagem else 0.0
    desvio = math.sqrt(max(variancia, 0.0))

    return {
        "soma_total": round(soma, 4),
        "media": round(media, 4),
        "maior_corrida": round(maior if contagem else 0.0, 4),
        "menor_corrida": round(menor if contagem else 0.0, 4),
        "total_corridas": contagem,
        "desvio_padrao": round(desvio, 4),
        "mediana": percentil_por_histograma(hist_total, 50, contagem),
        "percentil_25": percentil_por_histograma(hist_total, 25, contagem),
        "percentil_75": percentil_por_histograma(hist_total, 75, contagem),
        "percentil_90": percentil_por_histograma(hist_total, 90, contagem),
        "percentil_99": percentil_por_histograma(hist_total, 99, contagem),
        "distribuicao": dist_total,
    }


def executar(csv_path: str, total_linhas: int, num_processos: int, chunks_por_processo: int, carga_cpu: int) -> tuple[float, dict, int]:
    """Executa o benchmark para uma quantidade de processos."""
    intervalos = montar_intervalos(total_linhas, num_processos, chunks_por_processo)
    args = [
        (csv_path, inicio, fim, carga_cpu)
        for inicio, fim in intervalos
    ]

    t0 = time.perf_counter()
    with mp.Pool(processes=num_processos) as pool:
        parciais = pool.map(processar_intervalo, args)
    resultado = combinar(parciais)
    tempo = time.perf_counter() - t0

    return tempo, resultado, len(intervalos)


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


def grafico_tempo(processos, tempos, pasta):
    fig, ax = _fig_base("Tempo de Execução × Número de Processos", "Número de Processos", "Tempo (segundos)")
    ax.plot(processos, tempos, marker="o", linewidth=2.5, color=CORES[0], markersize=8, label="Tempo medido")
    for x, y in zip(processos, tempos):
        ax.annotate(f"{y:.2f}s", (x, y), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_tempo.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_speedup(processos, tempos, pasta):
    t1 = tempos[0]
    real = [t1 / t for t in tempos]
    ideal = processos[:]
    fig, ax = _fig_base("Speedup × Número de Processos", "Número de Processos", "Speedup")
    ax.plot(processos, ideal, linestyle="--", linewidth=1.5, color="gray", label="Speedup ideal")
    ax.plot(processos, real, marker="s", linewidth=2.5, color=CORES[1], markersize=8, label="Speedup medido")
    for x, y in zip(processos, real):
        ax.annotate(f"{y:.2f}x", (x, y), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_speedup.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_eficiencia(processos, tempos, pasta):
    t1 = tempos[0]
    eficiencia = [(t1 / t) / p * 100 for t, p in zip(tempos, processos)]
    fig, ax = _fig_base("Eficiência Paralela × Número de Processos", "Número de Processos", "Eficiência (%)")
    barras = ax.bar(processos, eficiencia, color=CORES[2], edgecolor="white", width=0.6)
    ax.axhline(100, linestyle="--", color="gray", linewidth=1.2, label="Eficiência ideal")
    ax.set_ylim(0, max(120, max(eficiencia) * 1.15))
    for bar, val in zip(barras, eficiencia):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2, f"{val:.1f}%", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(processos)
    ax.legend()
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_eficiencia.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_barras_tempo(processos, tempos, pasta):
    fig, ax = _fig_base("Comparativo de Tempo por Configuração", "Número de Processos", "Tempo (segundos)")
    barras = ax.bar([str(p) for p in processos], tempos, color=CORES[:len(processos)], edgecolor="white", width=0.55)
    for bar, val in zip(barras, tempos):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01, f"{val:.2f}s", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_barras_tempo.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_distribuicao(distribuicao: dict, total_corridas: int, pasta: str):
    labels = list(distribuicao.keys())
    valores = list(distribuicao.values())
    pcts = [v / total_corridas * 100 if total_corridas > 0 else 0 for v in valores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribuição de Corridas por Faixa de Distância", fontsize=14, fontweight="bold")

    ax1.pie(valores, labels=labels, colors=CORES[:len(labels)], autopct="%1.1f%%", startangle=140, pctdistance=0.82)
    ax1.set_title("Proporção (%)", fontsize=11)

    barras = ax2.barh(labels, pcts, color=CORES[:len(labels)], edgecolor="white")
    ax2.set_xlabel("% das corridas", fontsize=11)
    ax2.set_title("Distribuição (%)", fontsize=11)
    ax2.set_xlim(0, max(pcts) * 1.18 if pcts else 1)
    for bar, pct, qtd in zip(barras, pcts, valores):
        ax2.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2, f"{pct:.1f}%  ({qtd:,})", va="center", fontsize=9)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.4)

    fig.tight_layout()
    path = os.path.join(pasta, "grafico_distribuicao.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_estatisticas(resultado: dict, pasta: str):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title("Resumo Estatístico das Distâncias (milhas)", fontsize=14, fontweight="bold", pad=12)

    p25 = resultado["percentil_25"]
    med = resultado["mediana"]
    p75 = resultado["percentil_75"]
    p90 = resultado["percentil_90"]

    ax.barh(0, p75 - p25, left=p25, height=0.4, color=CORES[0], alpha=0.7, label="IQR (P25–P75)")
    ax.vlines(med, -0.2, 0.2, color="white", linewidth=3, zorder=5)
    ax.vlines(med, -0.2, 0.2, color=CORES[1], linewidth=2, zorder=6, label=f"Mediana ≈ {med:.2f} mi")
    ax.hlines(0, resultado["menor_corrida"], p25, color=CORES[0], linewidth=2)
    ax.hlines(0, p75, p90, color=CORES[0], linewidth=2)
    ax.vlines([resultado["menor_corrida"], p90], -0.12, 0.12, color=CORES[0], linewidth=2)

    for valor, label, offset in [
        (resultado["menor_corrida"], "Mín", -0.32),
        (p25, "P25", -0.32),
        (med, "P50", 0.32),
        (p75, "P75", -0.32),
        (p90, "P90", 0.32),
        (resultado["media"], "Média", 0.52),
    ]:
        ax.annotate(f"{label}\n{valor:.2f}", xy=(valor, 0), xytext=(valor, offset), ha="center", fontsize=8,
                    arrowprops=dict(arrowstyle="-", color="gray", lw=0.8))

    ax.set_yticks([])
    ax.set_xlabel("Distância (milhas)", fontsize=12)
    ax.set_xlim(resultado["menor_corrida"] * 0.8, max(resultado["percentil_99"] * 1.5, 1))
    ax.grid(True, axis="x", linestyle="--", alpha=0.4)
    ax.legend(loc="upper right")
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_estatisticas.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Benchmark paralelo do NYC Yellow Taxi Trip Data")
    parser.add_argument("--max-processos", "-m", type=int, default=min(mp.cpu_count(), 8),
                        help="Máximo de processos a testar")
    parser.add_argument("--repeticoes", "-r", type=int, default=REPETICOES,
                        help="Número de repetições por configuração")
    parser.add_argument("--chunks-por-processo", "-c", type=int, default=4,
                        help="Quantidade de chunks por processo. Maior valor melhora balanceamento, mas aumenta overhead")
    parser.add_argument("--carga-cpu", type=int, default=20,
                        help="Repetições de cálculo extra por registro. Use 0 para desativar")
    parser.add_argument("--csv", default=CSV_FILE,
                        help="Caminho do arquivo CSV")
    args = parser.parse_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    max_p = max(1, args.max_processos)
    processos_list = sorted(set(
        [1] + [2**i for i in range(1, int(log2(max(max_p, 2))) + 1) if 2**i <= max_p]
    ))
    if max_p not in processos_list:
        processos_list.append(max_p)

    print("=" * 72)
    print("  NYC Yellow Taxi  —  BENCHMARK OTIMIZADO")
    print(f"  Configurações       : {processos_list} processos")
    print(f"  Repetições          : {args.repeticoes} por configuração")
    print(f"  Chunks por processo : {args.chunks_por_processo}")
    print(f"  Carga CPU           : {args.carga_cpu}")
    print("=" * 72)

    print("\n  Contando linhas do CSV... ", end="", flush=True)
    t0 = time.perf_counter()
    total_linhas = contar_linhas_csv(csv_path)
    t1 = time.perf_counter()
    print(f"OK  ({total_linhas:,} linhas, {t1 - t0:.2f}s)")
    print("  Observação: essa contagem não entra no tempo comparativo do benchmark.")

    resultados_tempo = {}
    chunks_por_config = {}
    resultado_final = None

    for n in processos_list:
        tempos_exec = []
        print(f"\n  Aquecimento com {n:2d} processo(s)...", end=" ", flush=True)
        _, _, qtd_chunks = executar(csv_path, total_linhas, n, args.chunks_por_processo, args.carga_cpu)
        print(f"OK ({qtd_chunks} chunks)")

        print(f"  Testando    com {n:2d} processo(s)...", end=" ", flush=True)
        for rep in range(args.repeticoes):
            t, res, qtd_chunks = executar(csv_path, total_linhas, n, args.chunks_por_processo, args.carga_cpu)
            tempos_exec.append(t)
            resultado_final = res
            chunks_por_config[n] = qtd_chunks
            print(f"[rep {rep + 1}: {t:.3f}s]", end=" ", flush=True)

        tempos_ordenados = sorted(tempos_exec)
        tempo_mediana = tempos_ordenados[len(tempos_ordenados) // 2]
        resultados_tempo[n] = round(tempo_mediana, 6)
        print(f"  → mediana: {tempo_mediana:.4f}s")

    t_seq = resultados_tempo[1]
    print("\n" + "=" * 72)
    print(f"  {'Processos':>10}  {'Tempo (s)':>12}  {'Speedup':>10}  {'Eficiência':>12}  {'Chunks':>8}")
    print("  " + "-" * 68)
    for n in processos_list:
        t = resultados_tempo[n]
        sp = t_seq / t
        ef = sp / n * 100
        print(f"  {n:>10}  {t:>12.4f}  {sp:>10.3f}x  {ef:>11.1f}%  {chunks_por_config[n]:>8}")
    print("=" * 72)

    if resultado_final:
        print(f"\n  {'─' * 58}")
        print("  MÉTRICAS DAS CORRIDAS")
        print(f"  {'─' * 58}")
        print(f"  {'Total de corridas':<30}: {resultado_final['total_corridas']:>14,}")
        print(f"  {'Soma total (mi)':<30}: {resultado_final['soma_total']:>14,.4f}")
        print(f"  {'Média (mi)':<30}: {resultado_final['media']:>14.4f}")
        print(f"  {'Mediana aprox. (mi)':<30}: {resultado_final['mediana']:>14.4f}")
        print(f"  {'Maior corrida (mi)':<30}: {resultado_final['maior_corrida']:>14.4f}")
        print(f"  {'Menor corrida (mi)':<30}: {resultado_final['menor_corrida']:>14.4f}")
        print(f"  {'Desvio padrão (mi)':<30}: {resultado_final['desvio_padrao']:>14.4f}")

    ps = processos_list
    ts = [resultados_tempo[n] for n in ps]
    bench_data = {
        "processos": ps,
        "tempos": ts,
        "speedups": [round(t_seq / resultados_tempo[n], 4) for n in ps],
        "eficiencias": [round((t_seq / resultados_tempo[n]) / n * 100, 2) for n in ps],
        "repeticoes": args.repeticoes,
        "chunks_por_processo": args.chunks_por_processo,
        "carga_cpu": args.carga_cpu,
        "hist_bin_width": HIST_BIN_WIDTH,
        "hist_max": HIST_MAX,
        "observacao": "Tempos calculados pela mediana das repetições. CSV não é carregado inteiro em memória. Percentis estimados por histograma.",
        "chunks_por_configuracao": {str(k): v for k, v in chunks_por_config.items()},
        "metricas_corridas": resultado_final or {},
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

    if resultado_final:
        grafico_distribuicao(resultado_final["distribuicao"], resultado_final["total_corridas"], OUTPUT_DIR)
        grafico_estatisticas(resultado_final, OUTPUT_DIR)

    print(f"\n  ✅ Benchmark concluído! Gráficos em ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
