from src.utils import create_tables, seed_tables, ensure_schema, drop_tables


if __name__ == '__main__':
    schema = ensure_schema(drop_if_exists=False)
    drop_tables(schema)
    create_tables(schema)
    seed_tables(schema)




