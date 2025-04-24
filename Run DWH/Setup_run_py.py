import os
import re
import pyodbc
from dotenv import load_dotenv
from contextlib import closing
from rich import print 

# -------- env load and setup ------
load_dotenv()
driver_name = os.getenv('DB_DRIVER')
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
connection_string= f"DRIVER={driver_name};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
Go_re_pattern = re.compile(r"\s+GO\s+|GO\s+|GO")

# --------- script locations --------
SETUP_SCRIPT_LOCATION = r"Setup DWH\Setup.sql"
MAIN_SCRIPT_LOCATION = r"Run DWH\MAIN.sql"
BRONZE_SCRIPT_LOCATION = r"Layers\BRONZE LAYER.sql"
SILVER_SCRIPT_LOCATION = r"Layers\SILVER LAYER.sql"
GOLD_SCRIPT_LOCATION = r"Layers\GOLD LAYER.sql"

def load_schema(script:str):
    """
    Load a schema into the database. The script is read and executed line by line."""
    with open(script,'r') as clean_script:
        script = ''
        for line in clean_script:
            if not Go_re_pattern.match(line):
                script += line
        return script
            
        
def load_data_in_db():
    """ Load data into the database using the provided SQL scripts."""
    
    # open the connnection to the database and close it when done
    with closing(pyodbc.connect(connection_string)) as conn:
        cursor = conn.cursor()
        
        for script in [BRONZE_SCRIPT_LOCATION, SILVER_SCRIPT_LOCATION, GOLD_SCRIPT_LOCATION, MAIN_SCRIPT_LOCATION]:
            try:
                cursor.execute(
                    f"{load_schema(script)}"
                )
                
                for source, message in cursor.messages:
                    print(f"source: {source} | Message: {message}")
                cursor.messages.clear()
                # commit the changes to the database
                conn.commit() 
                print(f"{script.split('\\')[-1]}: Script Executed Successfully.")
            except Exception as e:
                
                # rollback the changes if there is an error
                conn.rollback()
                print(f"Error with {script.split('\\')[-1]}: Script as {e}")


if __name__ == "__main__":
    load_data_in_db()
    # print(load_schema(BRONZE_SCRIPT_LOCATION))