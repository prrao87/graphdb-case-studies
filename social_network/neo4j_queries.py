"""
Run custom Cypher queries on Neo4j graph to answer questions based on business case.
"""
from neo4j import GraphDatabase, BoltDriver


def query1(driver: BoltDriver) -> None:
    "Who are the top 3 most-followed persons in the network?"
    with driver.session() as session:
        query = """
            MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
            RETURN person.personID AS personID, size(collect(follower.personID)) AS numFollowers
            ORDER BY numFollowers DESC LIMIT 3
        """
        result = session.run(query)
        print(f"\nQuery 1:\n {query}")
        print(f"Top 3 most-followed persons:\n{result.data()}")


def query2(driver: BoltDriver) -> None:
    "In which city does the most-followed person in the network live?"
    with driver.session() as session:
        query = """
            MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
            WITH person, COLLECT(follower.personID) AS followers
            ORDER BY size(followers) DESC LIMIT 1
            MATCH (person) -[:LIVES_IN]-> (city:City)
            RETURN person.personID AS person, size(followers) AS numFollowers, city.name AS city
        """
        result = session.run(query)
        print(f"\nQuery 2:\n {query}")
        print(f"City in which most-followed person lives:\n{result.data()}")


def query3(driver: BoltDriver, **params) -> None:
    "Which are the top 5 cities in a particular region of the world with the lowest average age in the network?"
    with driver.session() as session:
        query = """
            MATCH (p:Person) -[:LIVES_IN]-> (c:City) -[*..2]-> (reg:Region {name: $region})
            RETURN c.name AS city, c.country AS country, avg(p.age) AS averageAge
            ORDER BY averageAge LIMIT 5
        """
        print(f"\nQuery 3:\n {query}")
        result = session.run(query, params)
        print(
            f"5 countries with lowest average age in {params['region']}:\n{result.data()}"
        )


def query4(driver: BoltDriver, **params) -> None:
    """
    Which 3 countries in the network have the most people within a specified age range?
    """
    with driver.session() as session:
        query = """
            MATCH (p:Person)
            WHERE p.age > $age_lower AND p.age < $age_upper
            MATCH (p) -[*..2]-> (country:Country)
            RETURN country.name AS countries, count(country) AS personCounts
            ORDER BY personCounts DESC LIMIT 3
        """
        print(f"\nQuery 4:\n {query}")
        result = session.run(query, params)
        print(
            f"""
            3 Countries with the most people with age > {params['age_lower']}
            and < {params['age_upper']}:\n{result.data()}
            """
        )


def main() -> None:
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345"))
    query1(driver)
    query2(driver)
    query3(driver, region="East Asia")
    query3(driver, region="Latin America")
    query4(driver, age_lower=29, age_upper=46)


if __name__ == "__main__":
    main()
