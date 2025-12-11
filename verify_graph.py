from neo4j import GraphDatabase

# Configuration (Match your upload script)
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "Orion07192025!")

def verify_import():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        driver.verify_connectivity()
        with driver.session() as session:
            print("=== Graph Data Verification Report ===\n")

            # 1. Count Total Chunks
            result = session.run("MATCH (c:Chunk) RETURN count(c) AS count")
            total = result.single()["count"]
            print(f"Total Chunk Nodes: {total}")

            # 2. Check for Table Nodes (The critical fix)
            # We check for nodes that have the property type='table'
            result = session.run("MATCH (c:Chunk) WHERE c.type = 'table' RETURN count(c) AS count")
            tables = result.single()["count"]
            print(f"Table Chunks Found: {tables}")
            
            if tables == 0:
                print("❌ WARNING: No table chunks found. The import logic for tables may still be failing.")
            else:
                print("✅ SUCCESS: Table chunks are present.")

            # 3. Check for 'doc_id' property (Previously missing)
            result = session.run("MATCH (c:Chunk) WHERE c.doc_id IS NOT NULL RETURN count(c) AS count")
            doc_ids = result.single()["count"]
            print(f"Nodes with 'doc_id': {doc_ids}")

            # 4. Sample a Table Node's Metadata
            if tables > 0:
                print("\n--- Sample Table Metadata ---")
                result = session.run("""
                    MATCH (c:Chunk) 
                    WHERE c.type = 'table' 
                    RETURN c.chunk_id AS id, 
                           substring(c.html_content, 0, 50) AS html_snippet,
                           c.summary AS summary
                    LIMIT 1
                """)
                record = result.single()
                print(f"Chunk ID:   {record['id']}")
                print(f"Summary:    {record['summary']}")
                print(f"HTML Start: {record['html_snippet']}...")

    except Exception as e:
        print(f"Connection failed: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    verify_import()