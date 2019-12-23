import json
from grakn.client import GraknClient


def build_graph(keyspace_name):
    # Builds a Grakn graph within the specified keyspace
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace=keyspace_name) as session:
            load_location_into_grakn(session)
            load_connections_into_grakn(session)


def load_location_into_grakn(session):
    # Load in the required files containing location entity information for the graph
    for case in LOCATION_INPUTS:
        data = parse_input_json(case['file'])
        # Store unique names of countries and regions and their connections for populating within graph
        regions = list(set([item['region'] for item in data]))
        countries = list(set([item['country'] for item in data]))
        connections = list(set([(item['country'], item['region']) for item in data]))  # tuple (country, region)

        for region in regions:
            print(f"Inserting entity: {region}")
            query = insert_region_and_country('region', region)
            with session.transaction().write() as transaction:
                transaction.query(query)
                transaction.commit()

        for country in countries:
            print(f"Inserting entity: {country}")
            query = insert_region_and_country('country', country)
            with session.transaction().write() as transaction:
                transaction.query(query)
                transaction.commit()

        for loc in connections:
            print(f"Connected entities: {loc}")
            query = connect_country_and_region(loc)
            with session.transaction().write() as transaction:
                transaction.query(query)
                transaction.commit()


def load_connections_into_grakn(session):
    # Load in the required files and generate relevant connections for the graph
    for case in CONNECTION_INPUTS:
        data = parse_input_json(case['file'])
        for item in data:
            query = case['query'](item)
            with session.transaction().write() as transaction:
                print(query)
                transaction.query(query)
                transaction.commit()


def insert_region_and_country(location_type, location_name):
    # Insert region/country entity (unique by name)
    query = f'''
        insert ${location_type} isa {location_type}, has name "{location_name}";
    '''
    return query


def connect_country_and_region(location):
    # Insert relationships between country and the corresponding region from the dataset
    query = f'''
        match $country isa country, has name "{location[0]}";
        $region isa region, has name "{location[1]}";
        insert (in-region: $country, contains-country: $region) isa has-country;
    '''
    return query


def city_connections(location):
    # Insert city (unique by cityID) and its relationship with the corresponding country from the dataset
    query = f'''
        match $country isa country, has name "{location['country']}";
        insert $city isa city, has city-id {str(location['cityID'])}, has name "{location['city']}";
        (in-country: $city, contains-city: $country) isa has-city;
    '''
    return query


def parse_input_json(filename):
    # Return JSON data
    with open(filename) as f:
        data = json.load(f)
    return data


# The filenames and methods we wish to use in this graph build operation
LOCATION_INPUTS = [
    {
        "file": "data/city_in_region.json",
    },
]

CONNECTION_INPUTS = [
    {
        "file": "data/city_in_region.json",
        "query": city_connections
    },
]

if __name__ == "__main__":
    build_graph(keyspace_name="social_network")
