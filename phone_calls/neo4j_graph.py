"""
Use the Neo4j official Python bolt driver to populate a Neo4j graph
"""
import json
from neo4j import GraphDatabase
from neo4j.work.transaction import Transaction
from typing import Dict


class Neo4jConnection:
    def __init__(
        self, uri: str, user: str, password: str, filenames: Dict[str, str]
    ) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.filenames = filenames

    def close(self) -> None:
        self.driver.close()

    def run(self) -> None:
        with self.driver.session() as session:
            session.write_transaction(self._create_indexes_and_constraints)
            session.write_transaction(
                self._create_companies, self.filenames["companies"]
            )
            session.write_transaction(self._create_people, self.filenames["people"])
            session.write_transaction(
                self._create_contracts, self.filenames["contracts"]
            )
            session.write_transaction(self._create_calls, self.filenames["calls"])

    @staticmethod
    def _create_indexes_and_constraints(tx: Transaction) -> None:
        "Set indexes to improve performance of adding nodes as the graph gets larger"
        index_queries = [
            # indexes
            "CREATE INDEX company_name IF NOT EXISTS FOR (c:Company) ON (c.name) ",
            # constraints
            "CREATE CONSTRAINT IF NOT EXISTS ON (p:Person) ASSERT p.personID IS UNIQUE ",
        ]
        for query in index_queries:
            tx.run(query)

    @staticmethod
    def _create_companies(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data AS d
            MERGE (c:Company {name: d.name})
            """,
            data=data,
        )

    @staticmethod
    def _create_people(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data as d
            MERGE (p:Person {personID: d.phone_number})
              SET p.firstName = d.first_name, p.lastName = d.last_name, p.city = d.city, p.age = d.age
              SET p.fullName = d.first_name + ' ' + d.last_name
            """,
            data=data,
        )

    @staticmethod
    def _create_contracts(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data as d
            MATCH (p:Person {personID: d.person_id})
            MATCH (c:Company {name: d.company_name})
            MERGE (p) -[r:HAS_CONTRACT]-> (c)
            """,
            data=data,
        )

    @staticmethod
    def _create_calls(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data AS d
            MERGE (p1:Person {personID: d.caller_id})
            MERGE (p2:Person {personID: d.callee_id})
            WITH p1, p2, d, toUpper(replace(split(d.started_at, 'T')[0], '-', '_')) AS rel_type
              CALL apoc.merge.relationship(p1, 'CALL_' + rel_type,
                  {started_at: datetime(d.started_at), call_duration: d.duration}, NULL, p2,
                  {started_at: datetime(d.started_at), call_duration: d.duration}
              )
              YIELD rel
            RETURN d
            """,
            data=data,
        )


def parse_input_json(filename):
    with open(filename) as f:
        data = json.load(f)
    return data


if __name__ == "__main__":
    filenames = {
        "companies": "data/companies.json",
        "people": "data/people.json",
        "contracts": "data/contracts.json",
        "calls": "data/calls.json",
    }
    # Start connection and run build queries
    connection = Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="12345",
        filenames=filenames,
    )
    print("Building graph...")
    connection.run()
    connection.close()
    print("Finished! Successfully imported data into Neo4j!")
