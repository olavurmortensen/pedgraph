# PedGraph -- Multilayer network database for pedigree analysis

## Install

For now, we install via `pip`. Clone repo:

```bash
git clone https://github.com/olavurmortensen/pedgraph.git
cd pedgraph
```

Create a virtual environment and activate it:

```bash
python3 -m venv venv
activate venv/bin/activate
```

Install via pip:

```bash
pip install -e .
```

## Run Neo4j with Docker

The easiest way to run the Neo4j database is via Docker:

```bash
docker run -d -p7474:7474 -p7687:7687 --env=NEO4J_AUTH=none neo4j
```

## Build database

To build the database, use the `BuildDB()` class, supplying the URI to the Neo4j database (that should be running in a Docker container), and the path to a CSV pedigree.

Below, we import the `BuildDB()` in Python, and build the database using one of the test trees in the PedGraph project.

```python
>>> from pedgraph.BuildDB import BuildDB
>>> BuildDB('bolt://0.0.0.0:7687', 'pedgraph/test/test_data/test_tree2.csv')
```

Alternatively, you can run the script from the command-line.

```bash
python pedgraph/BuildDB.py --uri bolt://0.0.0.0:7687 --csv pedgraph/test/test_data/test_tree2.csv
```

## Basic database queries

Below, we us the Neo4j API to connect to the database.

```python
>>> from neo4j import GraphDatabase
>>> driver = GraphDatabase.driver('bolt://0.0.0.0:7687')
>>> session = driver.session()
```

Below we find all child-parent relationships, and return the ID of the child and parent of all these.

```
>>> result = session.run('MATCH (child:Person)-[:is_child]->(parent) RETURN child.ind, parent.ind')
>>> from pprint import pprint
>>> pprint(result.values())
[['1', '11'],
 ['5', '1'],
 ['5', '2'],
 ['6', '1'],
 ['6', '2'],
 ['7', '3'],
 ['7', '4'],
 ['8', '3'],
 ['8', '4'],
 ['9', '5'],
 ['9', '8'],
 ['10', '6'],
 ['10', '7']]
```

Below, we find all the ancestors of a particular person.

```python
>>> session.run('MATCH (p:Person {ind: "9"})-[:is_child*]->(a) RETURN a.ind').values()
[['8'], ['4'], ['3'], ['5'], ['2'], ['1'], ['11']]
```

Count how many descendants each person has.

```python
>>> session.run('MATCH (p:Person)-[:is_child*]->(a) RETURN a.ind, count(*)').values()
[['11', 5], ['2', 4], ['1', 4], ['4', 4], ['3', 4], ['8', 1], ['5', 1], ['7', 1], ['6', 1]]
```

See e.g. [here](https://neo4j.com/developer/cypher/) for more information about Neo4j queries.

## Reconstruct genealogy

**TODO**

