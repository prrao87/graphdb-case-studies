"""
Run custom Cypher queries on Neo4j graph to answer questions based on business case.
"""
from time import time
from neo4j import GraphDatabase, BoltDriver


def query_1(driver: BoltDriver, **params) -> None:
    with driver.session() as session:
        query = """
            MATCH (callee:Person {personID: $poi}) <-[r]- (caller:Person)
            WHERE r.started_at > datetime($timestamp)
            WITH DISTINCT caller AS callers
            RETURN collect(callers.personID) AS callers
        """
        print(f"\nQuery 1:\n {query}")
        result = session.run(query, params)
        print(f"Result:\n{result.data()}")


def query_2(driver: BoltDriver, **params) -> None:
    with driver.session() as session:
        query = """
            MATCH (c:Company {name: $company}) <-[:HAS_CONTRACT]- (suspect:Person {city: $city}) -[r1]-> (:Person)
            WHERE suspect.age > $suspect_age
            WITH suspect, r1.started_at AS patternDate
            MATCH (target:Person) <-[r2]- (suspect)
            WHERE target.age < $target_age AND r2.started_at > patternDate
            WITH DISTINCT target AS t
            RETURN collect(t.personID) AS targets
        """
        print(f"\nQuery 2:\n {query}")
        result = session.run(query, params)
        print(f"Result:\n{result.data()}")


def query_3(driver: BoltDriver, **params) -> None:
    with driver.session() as session:
        query = """
            MATCH (p1:Person {personID: $person1}) -[:HAS_CONTRACT]-> (c:Company {name: $company})
            MATCH (p2:Person {personID: $person2}) -[:HAS_CONTRACT]-> (c)
            MATCH (p1) --> (contact:Person) <-- (p2)
            WITH DISTINCT contact AS c
            RETURN collect(c.personID) AS commonContacts
        """
        print(f"\nQuery 3:\n {query}")
        result = session.run(query, params)
        print(f"Result:\n{result.data()}")


def query_4(driver: BoltDriver, **params) -> None:
    with driver.session() as session:
        query = """
            MATCH (poi:Person {personID: $poi})
            MATCH (c:Company {name: $company}) <-- (p:Person) --> (poi)
            WITH collect(p) AS callers
              UNWIND callers AS caller
              UNWIND callers AS callee
            WITH *
              MATCH (caller) -- (callee)
            WITH DISTINCT caller AS c
              RETURN collect(c.personID) AS callers
        """
        print(f"\nQuery 4:\n {query}")
        result = session.run(query, params)
        print(f"Result:\n{result.data()}")


def query_5(driver: BoltDriver, **params) -> None:
    with driver.session() as session:
        query = """
            MATCH (company:Company {name: $company}) <-[:HAS_CONTRACT]- (customer:Person) -[called]-> (p:Person)
            WHERE customer.age > $age AND EXISTS (called.call_duration)
            RETURN customer.fullName as name, avg(called.call_duration) AS avgCallDuration
            ORDER BY avgCallDuration DESC LIMIT 3
        """
        print(f"\nQuery 5:\n {query}")
        result = session.run(query, params)
        print(f"Result:\n{result.data()}")


def main() -> None:
    start_time = time()
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345"))
    # Run required queries for inference
    query_1(driver, poi="+86 921 547 9004", timestamp="2018-09-14T17:18:49")
    query_2(driver, company='Telecom', city='London', suspect_age=50, target_age=20)
    query_3(driver, company='Telecom', person1='+7 171 898 0853', person2='+370 351 224 5176')
    query_4(driver, company='Telecom', poi='+48 894 777 5173')
    query_5(driver, company='Telecom', age=40)
    print(f"Ran queries in {time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
