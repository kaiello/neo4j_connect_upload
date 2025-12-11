import json
from neo4j import GraphDatabase

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "schema2.4"
NEO4J_PASSWORD = "Orion07192025!"  # Change this to your password
NEO4J_DATABASE = "neo4j" # The database to upload to (default is usually 'neo4j')
JSONL_FILE_PATH = "data.jsonl" # Change this to your local jsonl file path

def upload_data(tx, data):
    # Modify this query based on your JSON structure and desired graph model
    # This is a generic example assuming the JSON has 'id' and 'name' fields
    query = (
        "MERGE (n:Node {id: $id}) "
        "SET n.name = $name, n += $props"
    )
    # We separate specific fields and put the rest in 'props' if needed, 
    # or just map directly.
    # For this example, we assume the json object is passed as parameters
    tx.run(query, id=data.get('id'), name=data.get('name'), props=data)

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print(f"Connected to Neo4j. Uploading to database: '{NEO4J_DATABASE}'")

        with open(JSONL_FILE_PATH, 'r', encoding='utf-8') as f:
            with driver.session(database=NEO4J_DATABASE) as session:
                count = 0
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        session.execute_write(upload_data, data)
                        count += 1
                        if count % 100 == 0:
                            print(f"Processed {count} records...")
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON on line: {line[:50]}... Error: {e}")
                    except Exception as e:
                        print(f"Error uploading record: {data}. Error: {e}")
                
                print(f"Finished. Total records processed: {count}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()
