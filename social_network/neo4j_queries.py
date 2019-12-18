"""
Run custom Cypher queries on Neo4j graph to answer questions based on business case.
"""
from py2neo import Graph

uri = "bolt://localhost:7687"
# Start a Neo4j graph session and pass Cypher query strings to graph
graph = Graph(uri, user="neo4j", password="local")


def run_queries():
    # Run required queries for inference
    query1()
    query2()
    query3(region='East Asia')
    query3(region='Latin America')
    query4(age_lower=29, age_upper=46)


def query1(**params):
    """
    Who are the top 3 most-connected persons in the network?
    """
    query = """
        MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
        RETURN person.personID AS person, size(collect(follower.personID)) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3
    """
    print(f"\nQuery 1:\n {query}")
    result = graph.run(query, params)
    result = result.data()   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query2(**params):
    """
    In which city does the most-connected person in the network live?
    """
    query = """
        MATCH (follower:Person) -[:FOLLOWS]-> (person:Person)
        WITH person,
            collect(follower.personID) AS followers
        ORDER BY size(followers) DESC LIMIT 1
        MATCH (person) -[:LIVES_IN]-> (city:City)
        RETURN person.personID AS person, size(followers) AS numFollowers, city.name AS city
    """
    print(f"\nQuery 2:\n {query}")
    result = graph.run(query, params)
    result = result.data()   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query3(**params):
    """
    Which are the top 5 cities in a particular region of the world with the lowest average age in the network?
    """
    query = """
        MATCH (p:Person) -[:LIVES_IN]-> (c:City) -[*..2]-> (reg:Region {name: "%(region_name)s"})
        RETURN c.name AS city, c.country AS country, avg(p.age) AS averageAge
        ORDER BY averageAge LIMIT 5
    """ % {"region_name": params['region']}
    print(f"\nQuery 3:\n {query}")
    result = graph.run(query, params)
    result = result.data()   # Consume cursor and extract result
    print(f"Result:\n{result}")


def query4(**params):
    """
    Which 3 countries in the network have the most people within a specified age range?
    """
    query = """
        MATCH (p:Person)
        WHERE p.age > %(lower_lim)s AND p.age < %(upper_lim)s
        MATCH (p) -[*..2]-> (country:Country)
        RETURN country.name AS countries, count(country) AS personCounts
        ORDER BY personCounts DESC LIMIT 3
    """ % {
        "lower_lim": params['age_lower'], "upper_lim": params['age_upper']
    }
    print(f"\nQuery 4:\n {query}")
    result = graph.run(query, params)
    result = result.data()   # Consume cursor and extract result
    print(f"Result:\n{result}")


if __name__ == "__main__":
    run_queries()