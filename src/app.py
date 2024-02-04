import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Load the .env file variables
load_dotenv()

# Connect to the database
def connect_to_database():
    # Construct the connection string
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    # Create the engine with autocommit set to True
    engine = create_engine(connection_string).execution_options(autocommit=True)
    # Connect to the database
    connection = engine.connect()
    return engine, connection

# Execute SQL statements
def execute_sql_statements(engine, sql_statements):
    for sql_statement in sql_statements:
        engine.execute(sql_statement)

# Fetch data using Pandas
def fetch_data(engine, sql_query):
    data_df = pd.read_sql(sql_query, engine)
    return data_df

# SQL statements for creating tables
create_tables_sql = """
-- Create Publishers Table
CREATE TABLE publishers (
    publisher_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (publisher_id)
);

-- Create Authors Table
CREATE TABLE authors (
    author_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(50) NULL,
    last_name VARCHAR(100) NULL,
    PRIMARY KEY (author_id)
);

-- Create Books Table
CREATE TABLE books (
    book_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    total_pages INT NULL,
    rating DECIMAL(4, 2) NULL,
    isbn VARCHAR(13) NULL,
    published_date DATE,
    publisher_id INT NULL,
    PRIMARY KEY (book_id),
    CONSTRAINT fk_publisher FOREIGN KEY (publisher_id) REFERENCES publishers (publisher_id)
);

-- Create Book Authors Table
CREATE TABLE book_authors (
    book_id INT NOT NULL,
    author_id INT NOT NULL,
    PRIMARY KEY (book_id, author_id),
    CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books (book_id) ON DELETE CASCADE,
    CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors (author_id) ON DELETE CASCADE
);
"""

# SQL statements for inserting data
insert_data_sql = """
-- Your insert statements here
"""

# SQL query for fetching data
fetch_data_query = """
SELECT * FROM publishers;
"""

# Connect to the database
engine, connection = connect_to_database()

# Execute SQL statements to create tables
execute_sql_statements(engine, create_tables_sql.split(";"))

# Execute SQL statements to insert data
execute_sql_statements(engine, insert_data_sql.split(";"))

# Fetch and print the publishers data
publishers_df = fetch_data(engine, fetch_data_query)
print(publishers_df)

# Close the connection
connection.close()
