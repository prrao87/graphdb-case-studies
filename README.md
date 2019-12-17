# Working with Graph Databases to Build Knowledge Graphs
This repository contains example code for building knowledge graphs using the below graph databases and their client libraries.

* Neo4j
* Grakn

## What is a knowledge graph?
A knowledge graph uses an [*ontology*](https://blog.grakn.ai/what-is-an-ontology-c5baac4a2f6c) to bind connected data into a knowledge domain that can then be reasoned with programmatically. It makes use of specialized data structures defined within its underlying graph database to efficiently store and query connected data.

## Why use knowledge graphs?
Simply put, knowledge graphs allow us to represent highly connected data in a more natural way. By representing entities and how they are related to one another in the real world, we can ask intuitive questions such as "How does person X know person Y, and how many common interests do they share?". Since data is stored natively in a connected format, it becomes very efficient to traverse the paths between connected entities in the graph to answer these questions, regardless of the size of the graph.


## Datasets
The following datasets are used as case studies for each different graph database.
* `phone_calls` - Dataset of persons and their phone calls, obtained from the [Grakn documentation examples](https://dev.grakn.ai/docs/examples/phone-calls-overview). 
* `social_network` - Dataset of a simple, artificial social network that connects people and the places they live in.


## Neo4j
The world's most popular graph database, [Neo4j](https://neo4j.com/) uses a *Labelled Property Graph* model to store data natively as a graph. This is done by defining "nodes" that represent an entity in the real world (such as a person or a company) and "edges" that represent how these nodes are connected to one another via triples - for example, `person-A` `-[WORKS_AT]-` `company-B`. The primary benefit of using a property graph model is that it is intuitive and easy to understand - it is very simple to get a Neo4j graph model up and running.

## Grakn
Strictly speaking, Grakn is a "true" knowledge graph, in the sense that it models data using an entity-relationship model that makes use of multiple inheritance of hierarchies, hyper-entities and hyper-relations - this is referred to as a "[*hypergraph model*](https://blog.grakn.ai/modelling-data-with-hypergraphs-edff1e12edf0)".

A key feature of hypergraphs is that they generalize the notion of edges (from graph theory) to be composed of a collection of nodes (each of which represents a relationship). This allows not only entities to be related to one another, but also *relationships to be related to other relationships*. In addition, thanks to its ontology layer and schema definitions, Grakn can perform automated reasoning by utilizing user-defined rules to identify *[inferred relationships](https://blog.grakn.ai/inference-made-simple-f333fd8abce4)* that were not explicitly defined in the graph structure. 

## Installation

#### Neo4j
The installation instructions for Neo4j on Mac/Ubuntu/Windows are shown on their [web page](https://neo4j.com/docs/operations-manual/current/installation/).

#### Grakn
The installation instructions for Grakn on Mac/Ubuntu/Windows are shown on their [web page](https://dev.grakn.ai/docs/running-grakn/install-and-run).

#### Python clients
To more easily work with the raw data while building the graph, we use the Python clients for Neo4j and Grakn.

It is recommended to set up a virtual environment and install the required Python clients using ```requirements.txt``` as follows:

    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt

For further development, simply activate the existing virtual environment.

    source venv/bin/activate
