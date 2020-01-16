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
                _ = query1(transaction)
                _ = query2(transaction)
                _ = query3(transaction, region='East Asia')
                _ = query3(transaction, region='Latin America')
                _ = query4(transaction, age_lower=29, age_upper=46)


def query1(transaction):
    """
    Who are the top 3 most-followed persons in the network?
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
    # The object hierarchy needs to be looked up from the source code
    result = []

    for item in list(iterator):  # Consume ResponseIterator into a list

        # Convert AnswerGroup object --> Value and apply the number() method of this instance
        counts = item.answers()[0].number()

        # Apply the owner() method of AnswerGroup object to identify parent concepts
        # This returns an Attribute instance, on which we apply the value() method
        person = next(item.owner().attributes()).value()
        result.append({'personID': person, 'numFollowers': counts})

    sorted_results = sorted(result, key=lambda x: x['numFollowers'], reverse=True)
    print(f"Top 3 most-followed persons:\n{sorted_results[:3]}")

    return sorted_results


def query2(transaction):
    """
    In which city does the most-followed person in the network live?

    NOTE: This query is divided into two parts - we first identify the most-followed person
    (using the same query as query1) - and then use the ID of the person to match the city
    in which this person lives.
    """
    # Part 2a: Obtain ID of most-followed  (same as query1)
    top_3_followed = query1(transaction)
    top_1_followed = top_3_followed[0]['personID']
    print(f"Top most-followed person ID:\n{top_1_followed}")

    # Part 2b: Use ID of most-followed person and find their city of residence
    city_query = f'''
        match $person isa person, has person-id {top_1_followed};
        $residence(contains-residence: $city, in-city: $person) isa has-residence;
        get;
    '''
    print(f"\nQuery 2 (Obtain city in which most-followed person lives):\n {city_query}")
    iterator = transaction.query(city_query)
    answer = [ans.get('city') for ans in iterator][0]
    result = next(answer.attributes()).value()
    print(f"City in which most-followed person lives:\n{result}")

    return result


def query3(transaction, **params):
    """
    Which are the top 5 cities in a particular region of the world with the lowest average age in the network?
    """
    query = f'''
        match $person isa person, has age $age;
        $region isa region, has name "{params['region']}";
        $city isa city, has name $city-name;
        (contains-country: $region, in-region: $country) isa has-country;
        (contains-city: $country, in-country: $city) isa has-city;
        (contains-residence: $city, in-city: $person) isa has-residence;
        get $age, $city-name; group $city-name; mean $age;
    '''
    print(f"\nQuery 3:\n {query}")
    iterator = transaction.query(query)
    result = []

    for item in list(iterator):
        mean_age = item.answers()[0].number()   # Retrieve the number contained in this Value instance
        city = item.owner().value()   # Retrieve the value contained in this Attribute instance
        result.append({'city': city, 'averageAge': mean_age})

    sorted_results = sorted(result, key=lambda x: x['averageAge'])
    print(f"5 countries with lowest average age in {params['region']}:\n{sorted_results[:5]}")

    return sorted_results


def query4(transaction, **params):
    """
    Which 3 countries in the network have the most people within a specified age range?
    """
    query = f'''
        match $person isa person,
          has age > {params['age_lower']}, has age < {params['age_upper']};
        $country isa country, has name $country-name;
        (contains-city: $country, in-country: $city) isa has-city;
        (contains-residence: $city, in-city: $person) isa has-residence;
        get; group $country-name; count;
    '''
    print(f"\nQuery 4:\n {query}")
    iterator = transaction.query(query)
    result = []

    for item in list(iterator):  # Consume ResponseIterator into a list
        counts = item.answers()[0].number()
        country = item.owner().value()
        result.append({'country': country, 'personCounts': counts})

    sorted_results = sorted(result, key=lambda x: x['personCounts'], reverse=True)
    print(f"3 Countries with the most people with age > {params['age_lower']} and < {params['age_upper']}:  \
          \n{sorted_results[:3]}")
    return sorted_results


if __name__ == "__main__":
    run_queries()