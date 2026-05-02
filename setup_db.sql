-- Criação da tabela de exemplo para validação
CREATE TABLE dados_integridade (
    id SERIAL PRIMARY KEY,
    valor_referencia TEXT,
    hash_verificacao VARCHAR(64),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gerando 100.000 linhas de massa de dados para teste
INSERT INTO dados_integridade (valor_referencia, hash_verificacao)
SELECT 
    md5(random()::text), 
    md5(random()::text)
FROM generate_series(1, 100000);