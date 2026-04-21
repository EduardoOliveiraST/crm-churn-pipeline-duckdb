from pathlib import Path
import logging
import re

logger = logging.getLogger(__name__)

# Caminho do arquivo SQL onde ficam as queries (Q1 a Q4)
SQL_FILE = Path("sql/queries.sql")


def _load_queries() -> dict[str, str]:
    # Garante que o arquivo existe antes de tentar ler
    if not SQL_FILE.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    content = SQL_FILE.read_text(encoding="utf-8")

    # Quebra o arquivo usando comentários padrão: -- Q1, -- Q2, etc.
    pattern = r"^\s*--\s*(Q[1-4])\s*$"
    parts = re.split(pattern, content, flags=re.MULTILINE)

    queries = {}

    # O split gera algo assim:
    # [texto_antes, 'Q1', sql_q1, 'Q2', sql_q2, ...]
    # então percorro de 2 em 2 pegando nome + SQL
    for i in range(1, len(parts), 2):
        query_name = parts[i].strip()
        query_sql = parts[i + 1].strip().rstrip(";")
        queries[query_name] = query_sql

    # Validação básica: garantir que todas as queries esperadas existem
    expected = {"Q1", "Q2", "Q3", "Q4"}
    missing = expected - set(queries.keys())
    if missing:
        raise ValueError(f"Missing queries in {SQL_FILE}: {sorted(missing)}")

    return queries


def _wrap_count(query: str) -> str:
    # Envolve a query em um COUNT(*) pra validar o resultado final
    return f"SELECT COUNT(*) FROM ({query}) q"


def run_sql_validations(con) -> None:
    print("\n" + "=" * 50)
    print("🧠 RUNNING SQL VALIDATIONS")
    print("=" * 50 + "\n")

    # Esses valores não vieram do enunciado do teste.
    # Eu defini como checkpoints com base no resultado atual do pipeline,
    # depois de validar manualmente a lógica de cada query.
    # A ideia aqui é garantir que, se eu alterar alguma regra no código,
    # eu consiga detectar rapidamente qualquer regressão nas queries.

    expected_results = {
        "Q1": 20,  # quantidade de clientes que tiveram service após compra dentro de 365 dias
        "Q2": 5,   # número de canais retornados na agregação por campaign channel
        "Q3": 112, # inclui também clientes sem nenhuma interação (interpretação mais completa da regra)
        "Q4": 24,  # top 3 clientes por estado (total de estados * 3)
    }

    queries = _load_queries()

    # Executa cada query e compara com o esperado
    for name, query in queries.items():
        try:
            result = con.execute(_wrap_count(query)).fetchone()[0]
            expected = expected_results[name]
            status = "✅ PASS" if result == expected else "❌ FAIL"
            print(f"{name}: {result} (expected: {expected}) → {status}")
        except Exception as e:
            print(f"{name}: ❌ ERROR")
            print(f"   {str(e)}")

    print("\n" + "=" * 50)
    print("END OF SQL VALIDATIONS")
    print("=" * 50 + "\n")

    logger.info("SQL validations executed successfully")