import json
from py2neo import Graph

uri = "bolt://localhost:7687"


def build_graph(uri):
    # Start a Neo4j graph session and pass external data to build graph
    graph = Graph(uri, user="neo4j", password="local")
    # Set indexes and constraints to improve query performance
    set_indexes(graph)
    set_constraints(graph)
    # Build graph
    tx = graph.begin()
    for case in INPUTS:
        data = parse_input_json(case['file'])
        query = case['template'](data)
        if query:
            tx.run(query, {"data": data})
            tx.commit()
            print(f"Generated nodes/relationships using data from {case['file']}")
        tx = graph.begin()


def set_indexes(graph):
    # Create indexes on specific node properties to improve performance
    indexes = [
        "CREATE INDEX ON :Company(name)",
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


def company_template(data):
    # Query to create company nodes
    query = """
        UNWIND $data AS d
        MERGE (c:Company {name: d.name})
    """
    return query


def person_template(data):
    # Query to create person nodes and set its properties
    query = """
        UNWIND $data as d
        MERGE (p:Person {personID: d.phone_number})
          SET p.firstName = d.first_name, p.lastName = d.last_name, p.city = d.city, p.age = d.age
          SET p.fullName = d.first_name + ' ' + d.last_name
    """
    return query


def contract_template(data):
    # Query to create relationships between company and person
    query = """
        UNWIND $data as d
        MATCH (p:Person {personID: d.person_id})
        MATCH (c:Company {name: d.company_name})
        MERGE (p) -[r:HAS_CONTRACT]-> (c)
    """
    return query


def call_template(data):
    # Query to create relationships between persons who called each other
    query = """
        UNWIND $data AS d
        MERGE (p1:Person {personID: d.caller_id})
        MERGE (p2:Person {personID: d.callee_id})
        WITH p1, p2, d, toUpper(replace(split(d.started_at, 'T')[0], '-', '_')) AS rel_type
          CALL apoc.merge.relationship(p1, 'CALL_' + rel_type, {started_at: d.started_at, call_duration: d.duration}, 
                                       NULL, p2, {started_at: d.started_at, call_duration: d.duration}) YIELD rel
        RETURN d
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
        "file": "data/companies.json",
        "template": company_template
    },
    {
        "file": "data/people.json",
        "template": person_template
    },
    {
        "file": "data/contracts.json",
        "template": contract_template
    },
    {
        "file": "data/calls.json",
        "template": call_template
    },
]

if __name__ == "__main__":
    build_graph(uri)