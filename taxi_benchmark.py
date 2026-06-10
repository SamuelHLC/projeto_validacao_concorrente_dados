"""
=============================================================
  NYC Yellow Taxi Trip Data - BENCHMARK + GRÁFICOS
  Programação Concorrente e Distribuída
=============================================================
  Versão otimizada: leitura paralela por intervalos de bytes
  ─ Não carrega o CSV inteiro em memória como lista de dicts
  ─ Não envia milhões de linhas/dicionários para os workers
  ─ Reduz overhead de pickle (workers recebem apenas metadados)
  ─ Usa seek() para leitura direta por processo
  ─ Retorna apenas métricas agregadas (sem lista de distâncias)

  Uso:
    python taxi_benchmark.py
    python taxi_benchmark.py --max-processos 12
    python taxi_benchmark.py --processos 1,2,4,8,12
    python taxi_benchmark.py --repeticoes 5
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
import multiprocessing

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ─── Configuração ─────────────────────────────────────────
CSV_FILE   = "yellow_tripdata_2015-01.csv"
DIST_COL   = "trip_distance"
OUTPUT_DIR = "graficos_benchmark"
REPETICOES = 3

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
#  Utilitários — descobre metadados do CSV uma única vez
# ══════════════════════════════════════════════════════════

def inspecionar_csv(csv_path):
    """
    Lê apenas o cabeçalho do arquivo para descobrir:
      - índice da coluna trip_distance
      - posição de byte onde os dados começam (após o cabeçalho)
      - tamanho total do arquivo em bytes
    Custo: leitura de uma única linha.
    """
    with open(csv_path, "rb") as f:
        header_bytes = f.readline()
        data_start   = f.tell()
        f.seek(0, 2)                    # vai até o fim
        file_size = f.tell()

    header = header_bytes.decode("utf-8").strip().split(",")
    # Remove BOM se houver (UTF-8 BOM)
    if header and header[0].startswith("\ufeff"):
        header[0] = header[0].lstrip("\ufeff")

    try:
        col_idx = header.index(DIST_COL)
    except ValueError:
        raise RuntimeError(
            f"Coluna '{DIST_COL}' não encontrada no cabeçalho.\n"
            f"Colunas disponíveis: {header}"
        )

    return col_idx, data_start, file_size


def calcular_ranges(data_start, file_size, num_processos):
    """
    Divide o espaço de bytes da área de dados em `num_processos` intervalos
    aproximadamente iguais.  Os limites exatos são ajustados dentro do worker
    para não cortar linhas ao meio (lê até o próximo '\n').
    """
    total = file_size - data_start
    chunk = total / num_processos
    ranges = []
    for i in range(num_processos):
        ini = data_start + int(i * chunk)
        fim = data_start + int((i + 1) * chunk) if i < num_processos - 1 else file_size
        ranges.append((ini, fim))
    return ranges


# ══════════════════════════════════════════════════════════
#  Worker — lê diretamente do arquivo via seek()
# ══════════════════════════════════════════════════════════
#
# Por que isso é mais eficiente que a versão anterior?
#
#   Versão anterior:
#     1. Processo principal lê 100 % do CSV → lista de dicts (~GB de RAM)
#     2. Divide a lista em chunks
#     3. Envia cada chunk via pickle para os workers (overhead enorme)
#     4. Workers recebem os dados já prontos e apenas somam
#
#   Esta versão:
#     1. Processo principal lê apenas o cabeçalho (1 linha)
#     2. Envia para cada worker apenas 4 inteiros + 1 string (< 1 KB via pickle)
#     3. Cada worker abre o arquivo, faz seek() até seu intervalo e lê
#        somente a sua fatia — em paralelo com os demais
#     4. Worker retorna apenas ~10 números (métricas agregadas)
#
# Resultado: RAM proporcional a 1/N do arquivo por worker, overhead de
# pickle praticamente zero, I/O feito em paralelo pelos N processos.

def worker(args):
    """
    Parâmetros recebidos (tudo pequeno, serialização instantânea):
      csv_path  : caminho do arquivo
      byte_ini  : posição de início da fatia
      byte_fim  : posição de fim da fatia
      col_idx   : índice da coluna trip_distance
      data_start: byte onde os dados começam (para o 1º worker não pular cabeçalho)
    """
    csv_path, byte_ini, byte_fim, col_idx, data_start = args

    soma          = 0.0
    soma_quad     = 0.0
    contagem      = 0
    invalidas     = 0
    maior         = float("-inf")
    menor         = float("inf")
    distribuicao  = {label: 0 for _, _, label in FAIXAS}

    with open(csv_path, "rb") as f:
        # Avança até o início da fatia
        f.seek(byte_ini)

        # Se não é o início dos dados, avança até o próximo '\n'
        # para evitar leitura de linha cortada ao meio
        if byte_ini != data_start:
            f.readline()

        while True:
            pos  = f.tell()
            line = f.readline()
            if not line or pos >= byte_fim:
                break

            try:
                decoded = line.decode("utf-8", errors="replace").strip()
                if not decoded:
                    continue
                partes = decoded.split(",")
                dist   = float(partes[col_idx])
            except (ValueError, IndexError):
                invalidas += 1
                continue

            if dist <= 0:
                invalidas += 1
                continue

            soma      += dist
            soma_quad += dist * dist
            contagem  += 1
            if dist > maior: maior = dist
            if dist < menor: menor = dist

            for baixo, alto, label in FAIXAS:
                if baixo < dist <= alto:
                    distribuicao[label] += 1
                    break

    return {
        "soma":         soma,
        "soma_quad":    soma_quad,
        "contagem":     contagem,
        "invalidas":    invalidas,
        "maior":        maior if contagem > 0 else 0.0,
        "menor":        menor if contagem > 0 else 0.0,
        "distribuicao": distribuicao,
    }


# ══════════════════════════════════════════════════════════
#  Reduce — combina resultados parciais
# ══════════════════════════════════════════════════════════

def combinar(parciais):
    soma      = 0.0
    soma_quad = 0.0
    contagem  = 0
    invalidas = 0
    maior     = float("-inf")
    menor     = float("inf")
    dist_total = {label: 0 for _, _, label in FAIXAS}

    for p in parciais:
        soma      += p["soma"]
        soma_quad += p["soma_quad"]
        contagem  += p["contagem"]
        invalidas += p["invalidas"]
        if p["maior"] > maior: maior = p["maior"]
        if p["menor"] < menor: menor = p["menor"]
        for label in dist_total:
            dist_total[label] += p["distribuicao"].get(label, 0)

    media = soma / contagem if contagem > 0 else 0.0

    # Desvio padrão via fórmula de variância online (não precisa da lista completa)
    # Var = E[x²] - (E[x])²
    variancia = (soma_quad / contagem) - (media ** 2) if contagem > 0 else 0.0
    desvio    = math.sqrt(max(0.0, variancia))

    return {
        "total_corridas": contagem,
        "total_invalidas": invalidas,
        "soma_total":     round(soma,   4),
        "media":          round(media,  4),
        "maior_corrida":  round(maior,  4),
        "menor_corrida":  round(menor,  4),
        "desvio_padrao":  round(desvio, 4),
        "distribuicao":   dist_total,
        # Mediana e percentis exatos são omitidos do tempo de benchmark:
        # eles exigem ordenação global e coleta da lista completa de distâncias
        # de todos os workers, o que introduz overhead de comunicação O(N) e
        # invalida a medição pura do paralelismo.  Para obtê-los basta adicionar
        # uma fase extra pós-benchmark que coleta e ordena as distâncias.
        "nota_percentis": (
            "Percentis exatos removidos do benchmark para não introduzir "
            "overhead de comunicação O(N). Calculáveis em fase separada."
        ),
    }


# ══════════════════════════════════════════════════════════
#  Execução de uma rodada com N processos
# ══════════════════════════════════════════════════════════

def executar(csv_path, col_idx, data_start, file_size, num_processos):
    """Retorna (tempo_processamento, resultado_combinado)."""
    ranges = calcular_ranges(data_start, file_size, num_processos)
    args   = [
        (csv_path, ini, fim, col_idx, data_start)
        for ini, fim in ranges
    ]
    t0 = time.perf_counter()
    with multiprocessing.Pool(processes=num_processos) as pool:
        parciais = pool.map(worker, args)
    resultado = combinar(parciais)
    tempo = time.perf_counter() - t0
    return tempo, resultado


# ══════════════════════════════════════════════════════════
#  Validação de consistência entre configurações
# ══════════════════════════════════════════════════════════

def validar_resultado(ref, novo, n_proc, tolerancia=0.01):
    """
    Compara métricas-chave do resultado com N processos contra o resultado
    de referência (1 processo).  Aceita diferença de até `tolerancia` em
    valores contínuos (arredondamento de ponto flutuante em ordem diferente).
    """
    campos = ["total_corridas", "soma_total", "maior_corrida", "menor_corrida"]
    ok = True
    for campo in campos:
        v_ref = ref.get(campo, 0)
        v_new = novo.get(campo, 0)
        if isinstance(v_ref, (int, float)) and v_ref != 0:
            diff = abs(v_ref - v_new) / abs(v_ref)
            if diff > tolerancia:
                print(f"  [AVISO] {n_proc} proc — '{campo}' difere "
                      f"({v_ref} vs {v_new}, diff={diff:.4%})")
                ok = False
        elif v_ref != v_new:
            print(f"  [AVISO] {n_proc} proc — '{campo}' difere "
                  f"({v_ref} vs {v_new})")
            ok = False
    if ok:
        print(f"  [✓] {n_proc} processo(s): resultados consistentes com a referência.")
    return ok


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
    fig, ax = _fig_base(
        "Tempo de Execução × Número de Processos",
        "Número de Processos", "Tempo (segundos)",
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
    t1    = tempos[0]
    real  = [t1 / t for t in tempos]
    ideal = list(processos)
    fig, ax = _fig_base(
        "Speedup × Número de Processos",
        "Número de Processos", "Speedup",
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
        "Número de Processos", "Eficiência (%)",
    )
    barras = ax.bar(processos, eficiencia,
                    color=CORES[2], edgecolor="white", width=0.6)
    ax.axhline(100, linestyle="--", color="gray", linewidth=1.2,
               label="Eficiência ideal (100%)")
    ax.set_ylim(0, 130)
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
    fig, ax = _fig_base(
        "Comparativo de Tempo por Configuração",
        "Número de Processos", "Tempo (segundos)",
    )
    cores_barras = CORES[:len(processos)]
    barras = ax.bar([str(p) for p in processos], tempos,
                    color=cores_barras, edgecolor="white", width=0.55)
    for bar, val in zip(barras, tempos):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.005,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_barras_tempo.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_distribuicao(distribuicao: dict, total_corridas: int, pasta: str):
    labels  = list(distribuicao.keys())
    valores = list(distribuicao.values())
    pcts    = [v / total_corridas * 100 if total_corridas > 0 else 0 for v in valores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribuição de Corridas por Faixa de Distância",
                 fontsize=14, fontweight="bold")

    wedges, texts, autotexts = ax1.pie(
        valores, labels=labels, colors=CORES[:len(labels)],
        autopct="%1.1f%%", startangle=140, pctdistance=0.82,
    )
    for at in autotexts:
        at.set_fontsize(9)
    ax1.set_title("Proporção (%)", fontsize=11)

    barras = ax2.barh(labels, pcts, color=CORES[:len(labels)], edgecolor="white")
    ax2.set_xlabel("% das corridas", fontsize=11)
    ax2.set_title("Distribuição (%)", fontsize=11)
    ax2.set_xlim(0, max(pcts) * 1.18 if pcts else 1)
    for bar, pct, qtd in zip(barras, pcts, valores):
        ax2.text(
            bar.get_width() + 0.3,
            bar.get_y() + bar.get_height() / 2,
            f"{pct:.1f}%  ({qtd:,})",
            va="center", fontsize=9,
        )
    ax2.grid(True, axis="x", linestyle="--", alpha=0.4)

    fig.tight_layout()
    path = os.path.join(pasta, "grafico_distribuicao.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [✓] Salvo: {path}")


def grafico_estatisticas(resultado: dict, pasta: str):
    """Box-plot sintético com as estatísticas disponíveis."""
    media  = resultado["media"]
    desvio = resultado["desvio_padrao"]
    menor  = resultado["menor_corrida"]
    maior  = resultado["maior_corrida"]

    # Estimativas de percentis baseadas em média ± desvio
    # (exatas exigiriam ordenação global, removida do benchmark)
    p25_est = max(menor, media - 0.675 * desvio)
    p75_est = min(maior, media + 0.675 * desvio)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title("Resumo Estatístico das Distâncias (milhas)\n"
                 "(P25/P75 estimados via média±desvio; mediana exata removida do benchmark)",
                 fontsize=12, fontweight="bold", pad=12)

    # Caixa IQR estimada
    ax.barh(0, p75_est - p25_est, left=p25_est, height=0.4,
            color=CORES[0], alpha=0.7, label="IQR estimado (P25–P75)")
    # Média
    ax.vlines(media, -0.2, 0.2, color="white", linewidth=3, zorder=5)
    ax.vlines(media, -0.2, 0.2, color=CORES[1], linewidth=2,
              zorder=6, label=f"Média = {media:.2f} mi")
    # Bigodes
    ax.hlines(0, menor, p25_est, color=CORES[0], linewidth=2)
    ax.hlines(0, p75_est, maior, color=CORES[0], linewidth=2)
    ax.vlines([menor, maior], -0.12, 0.12, color=CORES[0], linewidth=2)

    for valor, label, offset in [
        (menor,  "Mín",   -0.35),
        (p25_est, "P25*", -0.35),
        (media,  "Média",  0.35),
        (p75_est, "P75*",  0.35),
        (maior,  "Máx",    0.35),
    ]:
        ax.annotate(
            f"{label}\n{valor:.2f}",
            xy=(valor, 0), xytext=(valor, offset),
            ha="center", fontsize=8,
            arrowprops=dict(arrowstyle="-", color="gray", lw=0.8),
        )

    ax.set_yticks([])
    ax.set_xlabel("Distância (milhas)", fontsize=12)
    ax.set_xlim(menor * 0.8, min(maior, media + 8 * desvio))
    ax.grid(True, axis="x", linestyle="--", alpha=0.4)
    ax.legend(loc="upper right")
    ax.text(0.01, 0.02, "* P25 e P75 estimados. Ver nota_percentis no JSON.",
            transform=ax.transAxes, fontsize=7, color="gray")
    fig.tight_layout()
    path = os.path.join(pasta, "grafico_estatisticas.png")
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
        default=min(multiprocessing.cpu_count(), 8),
        help="Máximo de processos; gera potências de 2 até esse valor (padrão: núcleos, até 8)",
    )
    parser.add_argument(
        "--processos", "-p",
        type=str,
        default=None,
        help="Lista manual de configurações, ex: 1,2,4,8,12",
    )
    parser.add_argument(
        "--repeticoes", "-r",
        type=int,
        default=REPETICOES,
        help=f"Repetições por configuração (padrão: {REPETICOES})",
    )
    args = parser.parse_args()
    repeticoes = max(1, args.repeticoes)

    # ── Monta lista de configurações ──
    if args.processos:
        try:
            processos_list = sorted(set(int(x.strip()) for x in args.processos.split(",")))
        except ValueError:
            print("[ERRO] --processos deve ser uma lista de inteiros, ex: 1,2,4,8,12")
            sys.exit(1)
    else:
        max_p = max(1, args.max_processos)
        processos_list = [1]
        p = 2
        while p <= max_p:
            processos_list.append(p)
            p *= 2
        if max_p not in processos_list:
            processos_list.append(max_p)
        processos_list = sorted(set(processos_list))

    # ── Verifica arquivo ──
    csv_path = CSV_FILE  # definida corretamente dentro de main()
    if not os.path.exists(csv_path):
        print(f"[ERRO] Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 62)
    print("  NYC Yellow Taxi  —  BENCHMARK (leitura por byte-range)")
    print(f"  Configurações : {processos_list} processos")
    print(f"  Repetições    : {repeticoes} por configuração")
    print(f"  Estratégia    : seek() por worker, sem carga em memória")
    print("=" * 62)

    # ── Inspeciona CSV (apenas o cabeçalho) ──
    print("\n  Inspecionando cabeçalho do CSV... ", end="", flush=True)
    try:
        col_idx, data_start, file_size = inspecionar_csv(csv_path)
    except RuntimeError as e:
        print(f"\n[ERRO] {e}")
        sys.exit(1)
    print(f"OK  (coluna '{DIST_COL}' = índice {col_idx}, "
          f"dados a partir do byte {data_start:,}, "
          f"tamanho total: {file_size / 1_000_000:.1f} MB)")

    resultados_tempo   = {}
    tempos_por_rep     = {}
    resultado_referencia = None

    for n in processos_list:
        tempos_exec = []
        print(f"\n  Testando {n:2d} processo(s)...", end=" ", flush=True)
        resultado_n = None
        for rep in range(repeticoes):
            t, res = executar(csv_path, col_idx, data_start, file_size, n)
            tempos_exec.append(round(t, 6))
            resultado_n = res
            print(f"[rep {rep + 1}: {t:.3f}s]", end=" ", flush=True)

        media_t = sum(tempos_exec) / repeticoes
        resultados_tempo[n] = round(media_t, 6)
        tempos_por_rep[n]   = tempos_exec

        if resultado_referencia is None:
            resultado_referencia = resultado_n
        else:
            validar_resultado(resultado_referencia, resultado_n, n)

        print(f"  → média: {media_t:.4f}s")

    # ── Tabela resumo ──
    t_seq = resultados_tempo[1]
    ps    = processos_list
    ts    = [resultados_tempo[n] for n in ps]

    print("\n" + "=" * 62)
    print(f"  {'Processos':>10}  {'Tempo (s)':>12}  {'Speedup':>10}  "
          f"{'Eficiência':>12}  {'Redução':>10}")
    print("  " + "-" * 60)
    for n, t in zip(ps, ts):
        sp = t_seq / t
        ef = sp / n * 100
        red = (1 - t / t_seq) * 100
        print(f"  {n:>10}  {t:>12.4f}  {sp:>10.3f}x  "
              f"{ef:>11.1f}%  {red:>9.1f}%")
    print("=" * 62)

    # ── Métricas das corridas ──
    if resultado_referencia:
        r = resultado_referencia
        print(f"\n  {'─' * 58}")
        print(f"  MÉTRICAS DAS CORRIDAS")
        print(f"  {'─' * 58}")
        print(f"  {'Total de corridas válidas':<32}: {r['total_corridas']:>12,}")
        print(f"  {'Total de linhas inválidas':<32}: {r['total_invalidas']:>12,}")
        print(f"  {'Soma total (mi)':<32}: {r['soma_total']:>12,.4f}")
        print(f"  {'Média (mi)':<32}: {r['media']:>12.4f}")
        print(f"  {'Maior corrida (mi)':<32}: {r['maior_corrida']:>12.4f}")
        print(f"  {'Menor corrida (mi)':<32}: {r['menor_corrida']:>12.4f}")
        print(f"  {'Desvio padrão (mi)':<32}: {r['desvio_padrao']:>12.4f}")

    # ── Salva JSON ──
    speedups    = [round(t_seq / resultados_tempo[n], 4) for n in ps]
    eficiencias = [round((t_seq / resultados_tempo[n]) / n * 100, 2) for n in ps]
    reducoes    = [round((1 - resultados_tempo[n] / t_seq) * 100, 2) for n in ps]

    bench_data = {
        "estrategia": {
            "descricao": "Leitura paralela por intervalos de bytes via seek()",
            "vantagens": [
                "Não carrega o CSV inteiro em memória",
                "Não envia linhas/dicionários para workers via pickle",
                "Cada worker lê diretamente sua fatia do arquivo",
                "Overhead de pickle mínimo: apenas metadados enviados",
                "I/O feito em paralelo pelos N processos",
                "Workers retornam apenas ~10 números (métricas agregadas)",
            ],
        },
        "processos":            ps,
        "tempos_medios":        ts,
        "tempos_por_repeticao": {str(n): tempos_por_rep[n] for n in ps},
        "speedups":             speedups,
        "eficiencias":          eficiencias,
        "reducoes_percentuais": reducoes,
        "repeticoes":           repeticoes,
        "metricas_corridas": {
            k: v for k, v in (resultado_referencia or {}).items()
        },
    }
    json_path = os.path.join(OUTPUT_DIR, "benchmark_dados.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(bench_data, f, indent=2, ensure_ascii=False)
    print(f"\n  Dados salvos em: {json_path}")

    # ── Gráficos ──
    print(f"\n  Gerando gráficos em '{OUTPUT_DIR}/'...")
    grafico_tempo(ps, ts, OUTPUT_DIR)
    grafico_speedup(ps, ts, OUTPUT_DIR)
    grafico_eficiencia(ps, ts, OUTPUT_DIR)
    grafico_barras_tempo(ps, ts, OUTPUT_DIR)

    if resultado_referencia:
        grafico_distribuicao(
            resultado_referencia["distribuicao"],
            resultado_referencia["total_corridas"],
            OUTPUT_DIR,
        )
        grafico_estatisticas(resultado_referencia, OUTPUT_DIR)

    print(f"\n  ✅ Benchmark concluído! Gráficos em ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    # Compatibilidade com Windows (multiprocessing com spawn)
    multiprocessing.freeze_support()
    main()