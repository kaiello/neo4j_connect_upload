import json
import time
import os
import glob
import argparse
from neo4j import GraphDatabase

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Orion07192025!" 
NEO4J_DATABASE = "neo4j"
DATA_DIRECTORY = "graph_rag_files" 
VECTOR_DIMENSION = 384
VECTOR_INDEX_NAME = "chunk_embedding_index"

def create_indexes(driver):
    with driver.session(database=NEO4J_DATABASE) as session:
        print("Ensuring constraints exist...")
        session.run("CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE")
        session.run("CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE")
        session.run("CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE")

        # Create Vector Index
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
            print("Waiting for vector index to be online...")
            while True:
                try:
                    status_result = session.run(f"SHOW VECTOR INDEXES WHERE name = '{VECTOR_INDEX_NAME}'")
                    record = status_result.single()
                    if record:
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

    # 2. Prepare Variables for Chunk/Table Merge
    meta = chunk_data.get('metadata', {})
    
    # Determine extra labels (e.g. if type is table, add :Table label)
    node_labels = "Chunk"
    if chunk_data.get('type') == 'table':
        node_labels += ":Table"

    # 3. Merge Chunk with comprehensive properties
    query_merge_chunk = f"""
        MERGE (c:Chunk {{id: $chunk_id}})
        SET c:{node_labels},
            c.text = $text,
            c.embedding = $embedding,
            c.page_number = $page_number,
            c.type = $type,
            c.doc_id = $doc_id,
            c.summary = $summary,
            c.html_content = $html_content,
            c.markdown_content = $markdown_content
        WITH c
        MATCH (d:Document {{id: $doc_id}})
        MERGE (c)-[:PART_OF]->(d)
    """
    
    tx.run(query_merge_chunk, 
        chunk_id=chunk_data['chunk_id'], 
        text=chunk_data['text'], 
        embedding=chunk_data.get('embedding', []),
        page_number=meta.get('page_number'),
        type=chunk_data.get('type', 'text'),
        doc_id=chunk_data['doc_id'],
        summary=meta.get('summary'),
        html_content=meta.get('html_content') or meta.get('text_as_html'),
        markdown_content=meta.get('markdown_content')
    )

    # Define a mapping for inconsistent labels
    LABEL_MAPPING = {
        "Person": "Kb_Person",    # Force 'Person' to become 'Kb_Person'
        "Organization": "Dbo_Organisation",
        "System": "Kb_System",
        "Program": "Kb_Program",
        "Fundingsource": "FundingSource", # Example of fixing title() artifacts
        "Project": "Kb_Project"
    }

    # 4. Process Extracted Nodes (Entities)
    for node in chunk_data.get('extracted_nodes', []):
        raw_label = node.get('label') or node.get('type') or "Unknown"
        
        # --- FIX 1: Normalize Label Casing ---
        # "organization" -> "Organization", "SYSTEM" -> "System"
        safe_label = "".join(x for x in raw_label if x.isalnum() or x == "_").title()
        
        # --- FIX 2: Apply Label Mapping (Override) ---
        if safe_label in LABEL_MAPPING:
            safe_label = LABEL_MAPPING[safe_label]
        
        # --- FIX 3: Normalize Node IDs (Prevent Duplicates like 'omid panahi' vs 'omid_panahi') ---
        # Lowercase AND replace spaces with underscores
        node_id = node['id'].strip().lower().replace(" ", "_")

        props = node.get('properties', {}).copy()
        if not props:
            # Fallback if properties are at top level
            reserved_keys = {'id', 'label', 'type'}
            for k, v in node.items():
                if k not in reserved_keys:
                    props[k] = v

        # Merge Entity
        query_entity = f"""
            MERGE (e:Entity {{id: $id}})
            SET e:{safe_label}, e += $props
        """
        tx.run(query_entity, id=node_id, props=props)

    # 5. Process Extracted Edges
    for edge in chunk_data.get('extracted_edges', []):
        source_id = edge['source']
        target_id = edge['target']
        
        rel_type = edge['type']
        safe_rel_type = "".join(x for x in rel_type if x.isalnum() or x == "_").upper()

        if source_id == chunk_data['chunk_id']:
            # Chunk -> Entity Relationship
            # Target is an Entity, so we MUST apply the same normalization: lowercase + underscores
            target_id_norm = target_id.strip().lower().replace(" ", "_")
            
            query_rel = f"""
                MATCH (c:Chunk {{id: $source_id}})
                MERGE (e:Entity {{id: $target_id}})
                MERGE (c)-[:{safe_rel_type}]->(e)
            """
            tx.run(query_rel, source_id=source_id, target_id=target_id_norm)
        else:
            # Entity -> Entity Relationship
            # Both are entities, so normalize BOTH IDs
            source_id_norm = source_id.strip().lower().replace(" ", "_")
            target_id_norm = target_id.strip().lower().replace(" ", "_")

            query_rel = f"""
                MERGE (s:Entity {{id: $source_id}})
                MERGE (t:Entity {{id: $target_id}})
                MERGE (s)-[:{safe_rel_type}]->(t)
            """
            tx.run(query_rel, source_id=source_id_norm, target_id=target_id_norm)

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

        if recursive:
            search_pattern = os.path.join(data_dir, "**", "*.jsonl")
        else:
            search_pattern = os.path.join(data_dir, "*.jsonl")
            
        jsonl_files = glob.glob(search_pattern, recursive=recursive)
        
        if not jsonl_files:
            print(f"No .jsonl files found in directory: {data_dir}")
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
                            if count % 50 == 0:
                                print(f"  Processed {count} chunks...")
                        except json.JSONDecodeError as e:
                            print(f"  Error decoding JSON on line: {line[:50]}... Error: {e}")
                        except Exception as e:
                            print(f"  Error uploading chunk: {data.get('chunk_id', 'Unknown')}. Error: {e}")
                    
                    print(f"Finished file {os.path.basename(file_path)}. Total: {count}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()