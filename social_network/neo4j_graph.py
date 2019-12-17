import json
from py2neo import Graph

uri = "bolt://localhost:7687"


def build_graph():
    # Start a Neo4j graph session and pass external data to build graph
    graph = Graph(uri, user="neo4j", password="local")
    # Set indexes and constraints to improve query performance
    set_indexes(graph)
    set_constraints(graph)
    # Build graph
    tx = graph.begin()
    for case in INPUTS:
        data = parse_input_json(case['file'])
        query = case['query'](data)
        if query:
            tx.run(query, {"data": data})
            tx.commit()
            print(f"Generated nodes/relationships using data from {case['file']}")
        tx = graph.begin()


def set_indexes(graph):
    # Create indexes on specific node properties to improve performance
    indexes = [
        "CREATE INDEX ON :City(cityID)",
        "CREATE INDEX ON :Country(name)",
        "CREATE INDEX ON :Region(name)",
    ]
    for index in indexes:
        graph.evaluate(index)


def set_constraints(graph):
    # Set uniqueness constraints on key properties
    constraints = [
        "CREATE CONSTRAINT ON (p:Person) ASSERT p.personID IS UNIQUE",
    ]
    for constraint in constraints:
        graph.evaluate(constraint)


def location_query(data):
    # Query to create city and region nodes/relationships
    query = """
        UNWIND $data AS d
        MERGE (c:City {cityID: d.cityID})
          SET c.name = d.city, c.country = d.country, c.region = d.region
        MERGE (co:Country {name: d.country})
        MERGE (re:Region {name: d.region})
        MERGE (c) -[:A_CITY_IN]-> (co)
        MERGE (co) -[:A_COUNTRY_IN]-> (re)
    """
    return query


def person_query(data):
    # Query to create person nodes and person-city relationships
    query = """
        UNWIND $data AS d
        MATCH (city:City {name: d.city, country: d.country})
        MERGE (p:Person {personID: d.personID})
          SET p.age = d.age
        MERGE (p) -[:LIVES_IN]-> (city)
    """
    return query


def connection_query(data):
    # Query to create person-person relationships
    query = """
        UNWIND $data AS d
        MATCH (p1:Person {personID: d.personID})
        MATCH (p2:Person {personID: d.connectionID})
        MERGE (p1) -[:FOLLOWS]-> (p2)
    """
    return query


def parse_input_json(filename):
    # Return JSON data
    with open(filename) as f:
        data = json.load(f)
    return data


# The filenames and methods we wish to use in this graph build operation
INPUTS = [
    {
        "file": "data/city_in_region.json",
        "query": location_query
    },
    {
        "file": "data/person_in_city.json",
        "query": person_query
    },
    {
        "file": "data/person_connections.json",
        "query": connection_query
    },
]

if __name__ == "__main__":
    build_graph()