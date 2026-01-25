import polars as ps
from psycopg2 import connect

def insert_parquet_into_postgres(parquet_file_path, table_name, postgres_connection_string):
    df = ps.read_parquet(parquet_file_path)
    conn = connect(postgres_connection_string)
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name};")
    cursor.executemany(
        f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))});",
        df.to_numpy().tolist()
    )
    conn.commit()
    cursor.close()
    conn.close()

# Example usage
if __name__ == "__main__":
    parquet_file_path = "D:\\My\\my study\\Courses\\ZoomCamp DTC\\week1\\green_tripdata_2025-11.parquet"
    table_name = "green_taxi_data"
    postgres_connection_string = "postgresql://postgres:postgres@localhost:5433/ny_taxi"
    
    insert_parquet_into_postgres(parquet_file_path, table_name, postgres_connection_string)

