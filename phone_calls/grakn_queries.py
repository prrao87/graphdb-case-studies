"""
Run custom Graql queries on Grakn graph to answer questions based on business case.
"""
from grakn.client import GraknClient

keyspace_name = "phone_calls"


def run_queries():
    # Builds a Grakn graph within the specified keyspace
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace=keyspace_name) as session:
            with session.transaction().write() as transaction:
                # Run required queries for inference
                query_1(transaction, poi="+86 921 547 9004", timestamp="2018-09-14T17:18:49")
                query_2(transaction, company='Telecom', city='London', suspect_age=50, target_age=20)
                query_3(transaction, company='Telecom', person1='+7 171 898 0853', person2='+370 351 224 5176')
                query_4(transaction, company='Telecom', poi='+48 894 777 5173')
                query_5(transaction, company='Telecom', age=20, operator='<')
                query_5(transaction, company='Telecom', age=40, operator='>')


def query_1(transaction, **params):
    query = f'''
        match
        $customer isa person, has phone-number $phone-number;
        $company isa company, has name "Telecom";
        (customer: $customer, provider: $company) isa contract;
        $target isa person, has phone-number "{params['poi']}";
        (caller: $customer, callee: $target) isa call, has started-at $started-at;
        $min-date == {params['timestamp']}; $started-at > $min-date;
        get $phone-number;
    '''
    print(f"\nQuery 1:\n {query}")
    iterator = transaction.query(query)
    answers = iterator.collect_concepts()
    result = [answer.value() for answer in answers]
    print(f"Result:\n{result}")


def query_2(transaction, **params):
    query = f'''
        match
        $suspect isa person, has city "{params['city']}", has age > {params['suspect_age']};
        $company isa company, has name "{params['company']}";
        (customer: $suspect, provider: $company) isa contract;
        $pattern-callee isa person, has age < {params['target_age']};
        (caller: $suspect, callee: $pattern-callee) isa call, has started-at $pattern-call-date;
        $target isa person, has phone-number $phone-number, has is-customer false;
        (caller: $suspect, callee: $target) isa call, has started-at $target-call-date;
        $target-call-date > $pattern-call-date;
        get $phone-number;
    '''
    print(f"\nQuery 2:\n {query}")
    iterator = transaction.query(query)
    answers = iterator.collect_concepts()
    result = [answer.value() for answer in answers]
    print(f"Result:\n{result}")


def query_3(transaction, **params):
    query = f'''
        match
        $common-contact isa person, has phone-number $phone-number;
        $company isa company, has name "{params['company']}";
        $customer-a isa person, has phone-number "{params['person1']}";
        $customer-b isa person, has phone-number "{params['person2']}";
        (customer: $customer-a, provider: $company) isa contract;
        (customer: $customer-b, provider: $company) isa contract;
        (caller: $customer-a, callee: $common-contact) isa call;
        (caller: $customer-b, callee: $common-contact) isa call;
        get $phone-number;
    '''
    print(f"\nQuery 3:\n {query}")
    iterator = transaction.query(query)
    answers = iterator.collect_concepts()
    result = [answer.value() for answer in answers]
    print(f"Result:\n{result}")


def query_4(transaction, **params):
    query = f'''
        match
        $target isa person, has phone-number "{params['poi']}";
        $company isa company, has name "{params['company']}";
        $customer-a isa person, has phone-number $phone-number-a;
        $customer-b isa person, has phone-number $phone-number-b;
        (customer: $customer-a, provider: $company) isa contract;
        (customer: $customer-b, provider: $company) isa contract;
        (caller: $customer-a, callee: $customer-b) isa call;
        (caller: $customer-a, callee: $target) isa call;
        (caller: $customer-b, callee: $target) isa call;
        get $phone-number-a, $phone-number-b;
    '''
    print(f"\nQuery 4:\n {query}")
    iterator = transaction.query(query)
    answers = iterator.collect_concepts()
    result = list(set([answer.value() for answer in answers]))
    print(f"Result:\n{result}")


def query_5(transaction, **params):
    query = f'''
        match
        $customer isa person, has age {params['operator']} {params['age']};
        $company isa company, has name "{params['company']}";
        (customer: $customer, provider: $company) isa contract;
        (caller: $customer, callee: $anyone) isa call, has duration $duration;
        get $duration; mean $duration;
    '''
    print(f"\nQuery 5:\n {query}")
    answer = list(transaction.query(query))
    result = 0
    if len(answer) > 0:
        result = answer[0].number()
    print(f"Result:\n{result}")


if __name__ == "__main__":
    run_queries()