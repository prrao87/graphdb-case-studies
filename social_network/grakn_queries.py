"""
Run custom Graql queries on Grakn graph to answer questions based on business case.
"""
from grakn.client import GraknClient

keyspace_name = "social_network"


def run_queries():
    # Builds a Grakn graph within the specified keyspace
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace=keyspace_name) as session:
            with session.transaction().write() as transaction:
                # Run required queries for inference
                query1(transaction)
                # query2()
                # query3(region='East Asia')
                # query3(region='Latin America')
                # query4(age_lower=29, age_upper=46)


def query1(transaction):
    """
    Who are the top 3 most-connected persons in the network?
    """
    query = f'''
        match $person isa person;
        (follower: $follower, followee: $person) isa connection;
        get; group $person; count;
    '''
    print(f"\nQuery 1:\n {query}")
    iterator = transaction.query(query)
    # To obtain the result for the "count" query, we need to look up the grakn python-client
    # source code: https://github.com/graknlabs/client-python/tree/master/grakn/service/Session
    result = []

    for item in list(iterator):  # Consume ResponseIterator into a list

        # Obtain a Value object from AnswerGroup object
        counts = item.answers()[0].number()   # Retrieve the number contained in this Value instance

        # Obtain Entity objects from AnswerGroup object
        person = next(item.owner().attributes()).value()   # Retrieve the value contained in this Attribute instance
        result.append({'person': person, 'numFollowers': counts})

    sorted_results = sorted(result, key=lambda x: x['numFollowers'], reverse=True)
    print(f"Top 3 most-followed persons:\n{sorted_results[:3]}")


def query2():
    """
    In which city does the most-connected person in the network live?
    """
    # print(f"\nQuery 2:\n {query}")
    # result = graph.run(query)
    # result = result.data()   # Consume cursor and extract result
    # print(f"Result:\n{result}")
    pass


def query3(**params):
    """
    Which are the top 5 cities in a particular region of the world with the lowest average age in the network?
    """
    # print(f"\nQuery 3:\n {query}")
    # result = graph.run(query, params)
    # result = result.data()   # Consume cursor and extract result
    # print(f"Result:\n{result}")
    pass


def query4(**params):
    """
    Which 3 countries in the network have the most people within a specified age range?
    """
    # print(f"\nQuery 4:\n {query}")
    # result = graph.run(query, params)
    # result = result.data()   # Consume cursor and extract result
    # print(f"Result:\n{result}")
    pass


if __name__ == "__main__":
    run_queries()