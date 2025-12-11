import json
import time
import os
import glob
import argparse
from neo4j import GraphDatabase

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Orion07192025!" # Updated password
NEO4J_DATABASE = "neo4j"
DATA_DIRECTORY = "graph_rag_files" # Directory containing your JSONL files
VECTOR_DIMENSION = 384
VECTOR_INDEX_NAME = "chunk_embedding_index"

def create_indexes(driver):
    with driver.session(database=NEO4J_DATABASE) as session:
        # Constraint for Chunk uniqueness
        session.run("CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE")
        
        # Constraint for Document uniqueness
        session.run("CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE")
        
        # Constraint for Entity uniqueness (using a generic Entity label for all extracted nodes)
        session.run("CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE")

        # Create Vector Index
        # Check if index exists first to avoid error or recreation
        result = session.run("SHOW VECTOR INDEXES")
        indexes = [record["name"] for record in result]
        
        if VECTOR_INDEX_NAME not in indexes:
            print(f"Creating vector index: {VECTOR_INDEX_NAME}")
            session.run(f"""
                CREATE VECTOR INDEX {VECTOR_INDEX_NAME} IF NOT EXISTS
                FOR (c:Chunk)
                ON (c.embedding)
                OPTIONS {{indexConfig: {{
                    `vector.dimensions`: {VECTOR_DIMENSION},
                    `vector.similarity_function`: 'cosine'
                }}}}
            """)
            # Wait for index to come online
            print("Waiting for vector index to be online...")
            while True:
                try:
                    status_result = session.run(f"SHOW VECTOR INDEXES WHERE name = '{VECTOR_INDEX_NAME}'")
                    record = status_result.single()
                    if record:
                        # Check for 'state' or 'status' key
                        status = record.get("state") or record.get("status")
                        if status == "ONLINE":
                            print("Vector index is ONLINE.")
                            break
                        print(f"Index status: {status}. Waiting...")
                    else:
                        print("Index not found yet. Waiting...")
                except Exception as e:
                    print(f"Error checking index status: {e}. Waiting...")
                
                time.sleep(1)
        else:
            print(f"Vector index {VECTOR_INDEX_NAME} already exists.")

def upload_chunk_data(tx, chunk_data):
    # 1. Merge Document
    tx.run("""
        MERGE (d:Document {id: $doc_id})
    """, doc_id=chunk_data['doc_id'])

    # 2. Merge Chunk and link to Document
    # We store the embedding as a property. 
    # Note: Neo4j vector index works on the property.
    tx.run("""
        MERGE (c:Chunk {id: $chunk_id})
        SET c.text = $text,
            c.embedding = $embedding,
            c.page_number = $page_number
        WITH c
        MATCH (d:Document {id: $doc_id})
        MERGE (c)-[:PART_OF]->(d)
    """, 
    chunk_id=chunk_data['chunk_id'], 
    text=chunk_data['text'], 
    embedding=chunk_data['embedding'],
    page_number=chunk_data.get('metadata', {}).get('page_number'),
    doc_id=chunk_data['doc_id']
    )

    # 3. Process Extracted Nodes
    for node in chunk_data.get('extracted_nodes', []):
        # Handle different JSON formats (label vs type)
        label = node.get('label') or node.get('type') or "Unknown"
        
        # Sanitize label to be safe (alphanumeric only)
        safe_label = "".join(x for x in label if x.isalnum() or x == "_")
        
        # Handle properties (nested 'properties' dict vs top-level keys)
        props = node.get('properties', {}).copy()
        if not props:
            # If no 'properties' key, treat other top-level keys as properties
            # Exclude reserved keys
            reserved_keys = {'id', 'label', 'type'}
            for k, v in node.items():
                if k not in reserved_keys:
                    props[k] = v

        query = f"""
            MERGE (e:Entity {{id: $id}})
            SET e:{safe_label}, e += $props
        """
        tx.run(query, id=node['id'], props=props)

    # 4. Process Extracted Edges
    for edge in chunk_data.get('extracted_edges', []):
        source_id = edge['source']
        target_id = edge['target']
        rel_type = edge['type']
        
        # Sanitize relationship type
        safe_rel_type = "".join(x for x in rel_type if x.isalnum() or x == "_").upper()

        # We need to handle two cases:
        # Case A: Source is the Chunk (MENTIONS relationship usually)
        # Case B: Source is an Entity (Entity-Entity relationship)
        
        if source_id == chunk_data['chunk_id']:
            # Link Chunk -> Entity
            query = f"""
                MATCH (c:Chunk {{id: $source_id}})
                MATCH (e:Entity {{id: $target_id}})
                MERGE (c)-[:{safe_rel_type}]->(e)
            """
            tx.run(query, source_id=source_id, target_id=target_id)
        else:
            # Link Entity -> Entity
            # Note: The entities must exist. They should have been created in step 3 
            # if they are in the same chunk. If they are cross-chunk, 
            # we might need to MERGE them here just in case, 
            # but usually 'extracted_nodes' contains all nodes mentioned in 'extracted_edges'.
            # To be safe, we match/merge.
            
            # However, if a node was mentioned in a previous chunk but not this one, 
            # it should already exist in the DB.
            
            query = f"""
                MERGE (s:Entity {{id: $source_id}})
                MERGE (t:Entity {{id: $target_id}})
                MERGE (s)-[:{safe_rel_type}]->(t)
            """
            tx.run(query, source_id=source_id, target_id=target_id)

def main():
    parser = argparse.ArgumentParser(description="Upload JSONL data to Neo4j for Graph RAG.")
    parser.add_argument("directory", nargs="?", default=DATA_DIRECTORY, help="Directory containing JSONL files")
    parser.add_argument("--recursive", "-r", action="store_true", help="Search recursively for .jsonl files")
    args = parser.parse_args()
    
    data_dir = args.directory
    recursive = args.recursive

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print(f"Connected to Neo4j. Uploading to database: '{NEO4J_DATABASE}'")
        
        create_indexes(driver)

        # Get all .jsonl files in the directory
        if recursive:
            search_pattern = os.path.join(data_dir, "**", "*.jsonl")
        else:
            search_pattern = os.path.join(data_dir, "*.jsonl")
            
        jsonl_files = glob.glob(search_pattern, recursive=recursive)
        
        if not jsonl_files:
            print(f"No .jsonl files found in directory: {data_dir} (Recursive: {recursive})")
            return

        print(f"Found {len(jsonl_files)} files to process: {[os.path.basename(f) for f in jsonl_files]}")

        with driver.session(database=NEO4J_DATABASE) as session:
            for file_path in jsonl_files:
                print(f"Processing file: {file_path}")
                with open(file_path, 'r', encoding='utf-8') as f:
                    count = 0
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            data = json.loads(line)
                            session.execute_write(upload_chunk_data, data)
                            count += 1
                            if count % 10 == 0:
                                print(f"  Processed {count} chunks in {os.path.basename(file_path)}...")
                        except json.JSONDecodeError as e:
                            print(f"  Error decoding JSON on line: {line[:50]}... Error: {e}")
                        except Exception as e:
                            print(f"  Error uploading chunk: {data.get('chunk_id')}. Error: {e}")
                    
                    print(f"Finished file {os.path.basename(file_path)}. Total chunks processed: {count}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()
