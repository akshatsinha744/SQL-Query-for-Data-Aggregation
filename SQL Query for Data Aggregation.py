import psycopg2
import pandas as pd

hostname = 'localhost'
database = 'Company_Set'
username = 'postgres'
pwd = '250857'
port_id = 5432
conn = None
cur = None

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()

    # Load the CSV file into a DataFrame
    file_path = r"C:\Users\davpt\OneDrive\Desktop\Akshat\question2_sales_data.csv"
    df = pd.read_csv(file_path)

    # Create table if it does not exist
    table_name = "q2_csv"
    create_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sale_id INT,
        product_id INT,
        quantity BIGINT,
        sale_date DATE
    );
    """
    cur.execute(create_script)

    # Clear the table to avoid duplication
    truncate_script = f"TRUNCATE TABLE {table_name};"
    cur.execute(truncate_script)

    # Copy data from CSV to the table
    with open(file_path, 'r') as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)

    # Commit the data insertion
    conn.commit()

    # Query to get total quantity sold in the last 30 days
    select_script = f"""
    SELECT 
        product_id, 
        SUM(quantity) AS total_quantity_sold
    FROM 
        {table_name}
    WHERE 
        sale_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 
        product_id
    ORDER BY 
        total_quantity_sold DESC;
    """
    
    # Execute the SELECT query
    cur.execute(select_script)
    results = cur.fetchall()
    
    # Print or process the results
    for row in results:
        print(row)

except Exception as error:
    print("Error:", error)

finally:
    # Close the cursor and connection
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
