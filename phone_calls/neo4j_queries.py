"""
Run custom Cypher queries on Neo4j graph to answer questions based on business case.
"""
from py2neo import Graph

uri = "bolt://localhost:7687"


def run_queries():
    # Start a Neo4j graph session and pass Cypher query strings to graph
    graph = Graph(uri, user="neo4j", password="local")
    # Run required queries for inference
    query_1(graph, poi="+86 921 547 9004", timestamp="2018-09-14T17:18:49")
    query_2(graph, company='Telecom', city='London', suspect_age=50, target_age=20)
    query_3(graph, company='Telecom', person1='+7 171 898 0853', person2='+370 351 224 5176')
    query_4(graph, company='Telecom', poi='+48 894 777 5173')
    query_5(graph, company='Telecom', age=20, operator='<')
    query_5(graph, company='Telecom', age=40, operator='>')


def query_1(graph, **params):
    query = """
        MATCH (callee:Person {personID:'%(poi)s'}) <-[r]- (caller:Person)
        WHERE r.started_at > datetime('%(timestamp)s')
        WITH DISTINCT caller AS callers
        RETURN collect(callers.personID) AS callers
    """ % {"poi": params['poi'], "timestamp": params['timestamp']}
    print(f"\nQuery 1:\n {query}")
    result = graph.run(query, params)
    result = result.data()[0]   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query_2(graph, **params):
    query = """
        MATCH (c:Company {name:'%(company)s'}) <-[:HAS_CONTRACT]- (suspect:Person {city:'%(city)s'}) -[r1]-> (:Person)
        WHERE suspect.age > %(suspect_age)s
        WITH suspect, r1.started_at AS patternDate
        MATCH (target:Person) <-[r2]- (suspect)
        WHERE target.age < %(target_age)s AND r2.started_at > patternDate
        WITH DISTINCT target AS t
        RETURN collect(t.personID) AS targets
    """ % {
        "company": params['company'], "city": params['city'],
        "suspect_age": params['suspect_age'], "target_age": params['target_age']
    }
    print(f"\nQuery 2:\n {query}")
    result = graph.run(query, params)
    result = result.data()   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query_3(graph, **params):
    query = """
        MATCH (p1:Person {personID:'%(person1)s'}) -[:HAS_CONTRACT]-> (c:Company {name:'%(company)s'})
        MATCH (p2:Person {personID:'%(person2)s'}) -[:HAS_CONTRACT]-> (c)
        MATCH (p1) --> (contact:Person) <-- (p2)
        WITH DISTINCT contact AS c
        RETURN collect(c.personID) AS commonContacts
    """ % {
        "company": params['company'], "person1": params['person1'], "person2": params['person2']
    }
    print(f"\nQuery 3:\n {query}")
    result = graph.run(query, params)
    result = result.data()[0]   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query_4(graph, **params):
    query = """
        MATCH (poi:Person {personID:'%(poi)s'})
        MATCH (c:Company {name:'%(company)s'}) <-- (p:Person) --> (poi)
        WITH collect(p) AS callers
        UNWIND callers AS caller
        UNWIND callers AS callee
        WITH *
        MATCH (caller) -- (callee)
        WITH DISTINCT caller AS c
        RETURN collect(c.personID) AS callers
    """ % {"company": params['company'], "poi": params['poi']}
    print(f"\nQuery 4:\n {query}")
    result = graph.run(query, params)
    result = result.data()[0]   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query_5(graph, **params):
    query = """
        MATCH (company:Company {name:'%(company)s'}) <-[:HAS_CONTRACT]- (customer:Person) -[called]-> (p:Person)
        WHERE customer.age %(operator)s %(age)s AND EXISTS (called.call_duration)
        RETURN avg(called.call_duration) AS avgCallDuration
    """ % {"company": params['company'], "age": params['age'], 'operator': params['operator']}
    print(f"\nQuery 5:\n {query}")
    result = graph.run(query, params)
    result = result.data()[0]   # Consume cursor and extract result
    print(f"Result:\n{result}")


if __name__ == "__main__":
    run_queries()