import json
from grakn.client import GraknClient


def build_graph(keyspace_name):
    # Builds a Grakn graph within the specified keyspace
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace=keyspace_name) as session:
            load_data_into_grakn(session)


def load_data_into_grakn(session):
    # Load in the required files and generate query transactions
    for k, v in INPUTS.items():
        data = parse_input_json(v['file'])
        for item in data:
            query = v['template'](item)
            with session.transaction().write() as transaction:
                print(query)
                transaction.query(query)
                transaction.commit()
        print("\nInserted {} items from {} into Grakn.\n".format(len(data), v['file']))


def company_template(company):
    # Insert company
    query = f'''
        insert $company isa company, has name "{company['name']}";
    '''
    return query


def person_template(person):
    # insert person
    person_query = f'''
        insert $person isa person, has phone-number "{person['phone_number']}"
    '''

    if "first_name" in person:
        person_query += f'''
            , has first-name "{person['first_name']}"
            , has last-name "{person['last_name']}"
            , has city "{person['city']}"
            , has age {str(person['age'])}
        '''
    person_query += ';'
    return person_query


def contract_template(contract):
    # Match company, match person and insert contract
    query = f'''
        match $company isa company, has name "{contract['company_name']}";
        $customer isa person, has phone-number "{contract['person_id']}";
        insert (provider: $company, customer: $customer) isa contract;
    '''
    return query


def call_template(call):
    # Match caller, match callee and then insert call
    query = f'''
        match $caller isa person, has phone-number "{call['caller_id']}";
        $callee isa person, has phone-number "{call['callee_id']}";
        insert $call(caller: $caller, callee: $callee) isa call;
        $call has started-at {call['started_at']};
        $call has duration {str(call['duration'])};
    '''
    return query


def parse_input_json(filename):
    # Return JSON data
    with open(filename) as f:
        data = json.load(f)
    return data


# The filenames and methods we wish to use in this graph build operation
INPUTS = {
    "companies":
        {
            "file": "data/companies.json",
            "template": company_template
        },
    "people":
        {
            "file": "data/people.json",
            "template": person_template
        },
    "contracts":
        {
            "file": "data/contracts.json",
            "template": contract_template
        },
    "calls":
        {
            "file": "data/calls.json",
            "template": call_template
        },
}

if __name__ == "__main__":
    build_graph(keyspace_name="phone_calls")
