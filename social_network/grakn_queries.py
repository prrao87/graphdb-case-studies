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
                # query1(transaction)
                query2(transaction)
                # query3(transaction, region='East Asia')
                # query3(transaction, region='Latin America')
                # query4(transaction, age_lower=29, age_upper=46)


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


def query2(transaction):
    """
    In which city does the most-connected person in the network live?

    NOTE: This query is divided into two parts - we first identify the most-followed person
    (using the same query as query1) - and then use the ID of the person to match the city
    in which this person lives.
    """
    # Part 2a: Obtain ID of most-followed person
    max_follower_query = f'''
        match $person isa person;
        (follower: $follower, followee: $person) isa connection;
        get; group $person; count;
    '''
    print(f"\nQuery 2a (Obtain ID of most-followed person):\n {max_follower_query}")
    iterator = transaction.query(max_follower_query)
    # To obtain the result for the "count" query, we need to look up the grakn python-client
    # source code: https://github.com/graknlabs/client-python/tree/master/grakn/service/Session
    result = []
    for item in list(iterator):
        # Obtain a Value object from AnswerGroup object
        counts = item.answers()[0].number()  
        # Obtain Entity objects from AnswerGroup object
        person = next(item.owner().attributes()).value()
        result.append({'person': person, 'numFollowers': counts})
    # Sort results to obtain the topmost-followed person
    sorted_results = sorted(result, key=lambda x: x['numFollowers'], reverse=True)[:1]
    most_followed_person = sorted_results[0]['person']
    print(f"Top most-followed person's ID:\n{most_followed_person}")

    # Part 2b: Use ID of most-followed person and find their city of residence
    city_query = f'''
        match $person isa person, has person-id {most_followed_person};
        $residence(contains-residence: $city, in-city: $person) isa has-residence;
        get $city;
    '''
    print(f"\nQuery 2b (Obtain city in which most-followed person lives):\n {city_query}")
    iterator = transaction.query(city_query)
    answer = iterator.collect_concepts()[0]
    city = next(answer.attributes()).value()
    print(f"City in which most-followed person lives:\n{city}")


def query3(**params):
    """
    Which are the top 5 cities in a particular region of the world with the lowest average age in the network?
    """
    pass


def query4(**params):
    """
    Which 3 countries in the network have the most people within a specified age range?
    """
    pass


if __name__ == "__main__":
    run_queries()