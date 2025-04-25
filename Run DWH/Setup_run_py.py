import os
import re
import pyodbc
from dotenv import load_dotenv
from contextlib import closing
from rich import print 
from typing import List, Any

# -------- env load and setup ------
load_dotenv()
DWH_DB_NAME = os.getenv('DATABASE1')
driver_name = os.getenv('DB_DRIVER')
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
connection_string= f"DRIVER={driver_name};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
Go_re_pattern = re.compile(r"\s+GO\s+|GO\s+|GO")

# --------- script locations --------
SETUP_SCRIPT_1_LOCATION = r"Setup DWH\Setup_script_1.sql"
SETUP_SCRIPT_2_LOCATION = r"Setup DWH\Setup_script_2.sql"
MAIN_SCRIPT_LOCATION = r"Run DWH\MAIN.sql"
BRONZE_SCRIPT_LOCATION = r"Layers\BRONZE LAYER.sql"
SILVER_SCRIPT_LOCATION = r"Layers\SILVER LAYER.sql"
GOLD_SCRIPT_LOCATION = r"Layers\GOLD LAYER.sql"

#----------- Execution order of scripts --------
SCRIPTS = [BRONZE_SCRIPT_LOCATION, SILVER_SCRIPT_LOCATION, GOLD_SCRIPT_LOCATION, MAIN_SCRIPT_LOCATION]


def read_sql_script(script:str):
    """Reads SQL script line by line and parses out 'GO' specific to only TSQL

    Args:
        script (str): File path to the sql script

    Returns:
        str: returns sql script without the word "GO"
    """
    with open(script,'r') as clean_script:
        script = ''
        for line in clean_script:
            if not Go_re_pattern.match(line):
                script += line
        return script
    


def check_database_exists(database_name:str):
    """ Checks if the database already exists 

    Args:
        database_name (str): The name of the database to check 

    Returns:
        bool: True for it already exists and False for it doesnt exist.
    """
    
    with closing(pyodbc.connect(connection_string)) as conn:
        cursor = conn.cursor()
        try:
            res = cursor.execute(
                '''
                SELECT COUNT(*) FROM sys.databases
                WHERE name = ?;
                '''
            ,database_name)
            exists = res.fetchone()[0]
            
            if exists:
                return True 
            else:
                return False
            
        except Exception as e:
            print(f"Error Checking {database_name} Existance")


def executor(script:str, conn_str:str):
    """Exectues the read sql script
    Args:
        script (str): Script to execute
    """
    with closing(pyodbc.connect(conn_str)) as conn:
        cursor = conn.cursor()
        try:
            output = cursor.execute(
                f"{read_sql_script(script)}"
            )
            
            for source, message in output.messages:
                print(f"source: {source} | Message: {message}")  
                
            output.messages.clear()
            # commit the changes to the database
            conn.commit() 
            print(f"{script.split('\\')[-1]}: Script Executed Successfully.")
        except Exception as e:
            
            # rollback the changes if there is an error
            #conn.rollback()
            print(f"Error with {script.split('\\')[-1]}: Script as {e}")
            
    
def type_check(object:Any, type_required):
    """_summary_

    Args:
        object (Any): object to check 
        type_required (_type_): required type the object must be 

    Raises:
        ValueError: raises error if the type is not whats needed

    Returns:
        _type_: True if the object type matches type_required otherwise raises a ValueError
    """
    if isinstance(object,type_required):
        return True
    raise ValueError
    

def connect_and_execute(scripts:List[str]|str, connection_str=connection_string):
    """ Connects to DB and Executes sql scripts.

    Args:
        scripts (List[str]|str): A list of paths to sql scripts to execute
    """
    # open the connnection to the database and close it when done
    type_check(scripts, list|str)
    if isinstance(scripts, List):
        for script in scripts:
            executor(script, conn_str=connection_str)
    elif isinstance(scripts,str):
        executor(scripts, conn_str=connection_str)
            

def main():
    if not check_database_exists(DWH_DB_NAME):
        connect_and_execute(SETUP_SCRIPT_1_LOCATION)
        if check_database_exists(DWH_DB_NAME):
            connect_and_execute(SETUP_SCRIPT_2_LOCATION)
            connect_and_execute(SCRIPTS)
        else:
            print(f"{DWH_DB_NAME}:DB doesnt exist")
    else:
        connect_and_execute(SCRIPTS)

if __name__ == "__main__":
    main()
