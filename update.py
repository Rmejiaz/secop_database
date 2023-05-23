import requests
from sodapy import Socrata
from neo4j import GraphDatabase
from datetime import datetime

# Neo4j connection details
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "your_username"
neo4j_password = "your_password"

# Socrata API details
socrata_domain = "www.datos.gov.co"
socrata_dataset_id = "jbjy-vk9h"
socrata_token = "your_api_token"  # Optional: Only required if you need authentication

# Initialize Neo4j driver
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# Connect to Socrata API
client = Socrata(socrata_domain, app_token=socrata_token)

# Retrieve last update timestamp from Neo4j (assuming it's stored in a property of a specific node)
with driver.session() as session:
    result = session.run("MATCH (n:LastUpdate) RETURN n.timestamp AS last_update")
    last_update_timestamp = result.single().get("last_update")

# Format last update timestamp
if last_update_timestamp:
    last_update_datetime = datetime.strptime(last_update_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
else:
    # Set an initial timestamp if it doesn't exist
    last_update_datetime = datetime(1970, 1, 1)

# Prepare Socrata API query with modified/created records since the last update
api_query = f"{socrata_dataset_id}.json?$where=last_modified_date > '{last_update_datetime.isoformat()}'"

# Retrieve data from Socrata API
results = client.get(api_query)

# Process and update data in Neo4j
with driver.session() as session:
    # Create an index on the 'id' property for faster updates
    session.run("CREATE INDEX ON :Node(id)")

    for data in results:
        # Extract the necessary data from the Socrata API response
        node_id = data.get("node_id")
        node_label = data.get("node_label")
        relationship_type = data.get("relationship_type")
        target_node_id = data.get("target_node_id")

        # Create or update nodes and relationships in Neo4j
        session.run(
            """
            MERGE (n:Node {id: $node_id})
            SET n.label = $node_label
            MERGE (t:Node {id: $target_node_id})
            MERGE (n)-[r:RELATIONSHIP_TYPE]->(t)
            """,
            node_id=node_id,
            node_label=node_label,
            relationship_type=relationship_type,
            target_node_id=target_node_id,
        )

# Update the last update timestamp in Neo4j
now = datetime.utcnow().isoformat() + "Z"
with driver.session() as session:
    session.run("MERGE (n:LastUpdate) SET n.timestamp = $timestamp", timestamp=now)

# Close the Neo4j driver and Socrata client
driver.close()
client.close()
