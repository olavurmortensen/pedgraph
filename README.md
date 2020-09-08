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
from pedgraph.BuildDB import BuildDB
BuildDB('bolt://0.0.0.0:7687', 'pedgraph/test/test_data/test_tree2.csv')
```

Alternatively, you can run the script from the command-line.

```bash
python pedgraph/BuildDB.py --uri bolt://0.0.0.0:7687 --csv pedgraph/test/test_data/test_tree2.csv
```


