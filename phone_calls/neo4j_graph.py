import json
from py2neo import Graph

uri = "bolt://localhost:7687"


def build_graph(uri):
    # Start a Neo4j graph session and pass external data to build graph
    graph = Graph(uri, user="neo4j", password="local")
    tx = graph.begin()
    for k, v in INPUTS.items():
        data = parse_input_json(v['file'])
        query = v['template'](data)
        tx.run(query, {"data": data})
        tx.commit()
        print(f"Generated nodes/relationships using data from {k}.json")
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
          SET p += d
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


def parse_input_json(filename):
    # Return JSON data
    with open(filename) as f:
        data = json.load(f)
    return data


# The filenames and methods we wish to use in this graph build operation
INPUTS = {
    "companies":
        {
            "file": "data/companies.json",
            "template": company_template
        },
    "people":
        {
            "file": "data/people.json",
            "template": person_template
        },
    "contracts":
        {
            "file": "data/contracts.json",
            "template": contract_template
        },
    # "calls":
    #     {
    #         "file": "data/calls.json",
    #         "template": "call_template"
    #     },
}

if __name__ == "__main__":
    build_graph(uri)