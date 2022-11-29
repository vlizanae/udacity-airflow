from .test import Test

registry = {
    'check_no_null': Test(
        query = """
            SELECT COUNT(*)
            FROM {table}
            WHERE {column} IS NULL
        """,
        result = "count\n-------\n0\n(1 row)",
        verification = lambda x, y: x == y
    ),
}