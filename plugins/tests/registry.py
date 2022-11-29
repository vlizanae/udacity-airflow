from .test import Test

registry = {
    'check_no_null': Test(
        query = """
            SELECT COUNT(*)
            FROM {table}
            WHERE {column} IS NULL
        """,
        verification = 'no_rows'
    ),
}