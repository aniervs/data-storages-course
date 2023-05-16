from src.console_ui import start_interaction
from src.utils import create_tables, seed_tables, ensure_schema, drop_tables


if __name__ == '__main__':
    # ans = input("Do you want to recreate the database (y/n)?")
    ans = 'n'
    if ans == 'y':
        schema = ensure_schema(drop_if_exists=True)
        drop_tables(schema)
        create_tables(schema)
        seed_tables(schema)
    else:
        schema = ensure_schema(drop_if_exists=False)
        start_interaction(schema)
