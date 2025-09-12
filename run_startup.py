from core.startup import setup_database, validate_database_schema

if __name__ == "__main__":
    db_path="jcb_db.db"
    setup_database(db_path)
    validate_database_schema(db_path)