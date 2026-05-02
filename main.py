import sqlite3
import threading
import time
import os

# Nome do arquivo do banco local no Codespace
DB_FILE = "dados_projeto.db"

def configurar_banco():
    """Cria o banco SQLite e popula com 100k registros para o teste"""
    print("Configurando banco de dados local (SQLite)...")
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS dados_integridade")
    cur.execute("""
        CREATE TABLE dados_integridade (
            id INTEGER PRIMARY KEY,
            valor_referencia TEXT,
            hash_verificacao TEXT
        )
    """)
    
    # Gerando massa de dados para validação
    dados = [(i, f"valor_ref_{i}", f"hash_origem_{i}") for i in range(1, 100001)]
    cur.executemany("INSERT INTO dados_integridade VALUES (?, ?, ?)", dados)
    conn.commit()
    conn.close()
    print("Banco pronto para o processamento!\n")

def validar_fatia(id_inicio, id_fim, thread_id):
    """Lógica de cada thread: valida integridade com carga de CPU"""
    try:
        # Cada thread abre sua própria conexão para evitar trava de IO
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT hash_verificacao FROM dados_integridade WHERE id BETWEEN ? AND ?", (id_inicio, id_fim))
        rows = cur.fetchall()
        
        for row in rows:
            # Simulação de processamento pesado de integridade (CPU Bound)
            # O loop for abaixo garante que o paralelismo seja eficiente no Python
            dado = row[0]
            for _ in range(800): 
                dado = hash(dado)
                
        cur.close()
        conn.close()
        # print(f"[Thread {thread_id}] Finalizou fatia {id_inicio} a {id_fim}")
    except Exception as e:
        print(f"Erro na Thread {thread_id}: {e}")

def rodar_teste(n_threads, total):
    """Divide o trabalho entre as threads e mede o tempo total"""
    fatia = total // n_threads
    threads = []
    inicio_cronometro = time.time()

    for i in range(n_threads):
        inicio = (i * fatia) + 1
        fim = (i + 1) * fatia
        t = threading.Thread(target=validar_fatia, args=(inicio, fim, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    return time.time() - inicio_cronometro

if __name__ == "__main__":
    TOTAL_REGISTROS = 100000 
    
    # 1. Garante que o ambiente tem dados
    configurar_banco()
    
    print(f"--- TESTE DE PERFORMANCE: VALIDAÇÃO DE {TOTAL_REGISTROS} REGISTROS ---")

    # 2. Execução Sequencial (1 Thread)
    # Representa o cenário antigo que o Marcelo comentou
    print("Executando Sequencialmente (1 Thread)...")
    tempo_seq = rodar_teste(1, TOTAL_REGISTROS)
    print(f"Tempo Sequencial: {tempo_seq:.4f} segundos")

    # 3. Execução Concorrente (4 Threads)
    # Representa a solução proposta pela dupla
    print(f"\nExecutando em Paralelo (4 Threads)...")
    tempo_con = rodar_teste(4, TOTAL_REGISTROS)
    print(f"Tempo Concorrente: {tempo_con:.4f} segundos")

    # 4. Cálculo do Ganho de Eficiência (Speedup)
    if tempo_con > 0:
        speedup = tempo_seq / tempo_con
        print("\n" + "="*45)
        print(f"RESULTADO FINAL:")
        print(f"SPEEDUP ALCANÇADO: {speedup:.2f}x")
        print(f"EFICIÊNCIA: {((1 - (tempo_con/tempo_seq)) * 100):.1f}% mais rápido")
        print("="*45)