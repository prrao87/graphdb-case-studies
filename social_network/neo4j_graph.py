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
                self._create_locations, self.filenames["locations"]
            )
            session.write_transaction(
                self._create_person_to_city, self.filenames["person_to_city"]
            )
            session.write_transaction(
                self._create_person_to_person, self.filenames["person_to_person"]
            )

    @staticmethod
    def _create_indexes_and_constraints(tx: Transaction) -> None:
        "Set indexes to improve performance of adding nodes as the graph gets larger"
        index_queries = [
            # indexes
            "CREATE INDEX city_id IF NOT EXISTS FOR (city:City) ON (city.cityID) ",
            "CREATE INDEX country_name IF NOT EXISTS FOR (country:Country) ON (country.name) ",
            "CREATE INDEX region_name IF NOT EXISTS FOR (region:Region) ON (region.name) ",
            # constraints
            "CREATE CONSTRAINT IF NOT EXISTS ON (p:Person) ASSERT p.personID IS UNIQUE",
        ]
        for query in index_queries:
            tx.run(query)

    @staticmethod
    def _create_locations(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data AS d
            MERGE (c:City {cityID: d.cityID})
            SET c.name = d.city, c.country = d.country, c.region = d.region
            MERGE (co:Country {name: d.country})
            MERGE (re:Region {name: d.region})
            MERGE (c) -[:A_CITY_IN]-> (co)
            MERGE (co) -[:A_COUNTRY_IN]-> (re)
            """,
            data=data,
        )

    @staticmethod
    def _create_person_to_city(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data AS d
            MATCH (city:City {name: d.city, country: d.country})
            MERGE (p:Person {personID: d.personID})
            SET p.age = d.age
            MERGE (p) -[:LIVES_IN]-> (city)
            """,
            data=data,
        )

    @staticmethod
    def _create_person_to_person(tx: Transaction, filename: str) -> None:
        data = parse_input_json(filename)
        tx.run(
            """
            UNWIND $data AS d
            MATCH (p1:Person {personID: d.personID})
            MATCH (p2:Person {personID: d.connectionID})
            MERGE (p1) -[:FOLLOWS]-> (p2)
            """,
            data=data,
        )


def parse_input_json(filename):
    with open(filename) as f:
        data = json.load(f)
    return data


if __name__ == "__main__":
    filenames = {
        "locations": "data/city_in_region.json",
        "person_to_city": "data/person_in_city.json",
        "person_to_person": "data/person_connections.json",
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
