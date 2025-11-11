import os
import math
import rdflib
from rdflib import Namespace, BNode, Literal, URIRef
import pandas as pd
import multiprocessing
import psycopg
import configparser

# Namespaces
RML = Namespace("http://w3id.org/rml/")
RR  = Namespace("http://www.w3.org/ns/r2rml#")
UB  = Namespace("http://swat.cse.lehigh.edu/onto/univ-bench.owl#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

g = rdflib.Graph()
g.parse('/home/jorge/proyectos/git/morph-kgc/examples/configuration-file/normalized_mapping.ttl', format='turtle')
config = configparser.ConfigParser()
config.read("default_config.ini")
conn = config.get("DataSource1", "db_url")

q_query = """
    PREFIX rml: <http://w3id.org/rml/>
    PREFIX rr: <http://www.w3.org/ns/r2rml#>
    PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>

    SELECT DISTINCT ?tm ?query ?sm ?pom WHERE {
        ?tm rml:logicalSource ?ls .
        ?ls rml:query ?query .
        ?tm rml:subjectMap ?sm .
        OPTIONAL {
            ?tm rml:predicateObjectMap ?pom . }
    }
    """

results = g.query(q_query)

mapping_graph = rdflib.Graph()
mapping_graph.bind("rml", RML)
mapping_graph.bind("rr", RR)
mapping_graph.bind("ub", UB)

#=== Functions ===

def row_counter(db_url: str, query: str):
    """
    return the number of rows in the query
    """
    # if the URL is in SQLAlchemy format (with +psycopg), we adapt it
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")
    try:
        conn = psycopg.connect(db_url)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subquery")
        row_number = cur.fetchone()[0]
        print(f"Rows number: {row_number}")
        cur.close()
        conn.close()

    except Exception as e:
        print("Error trying to connect or executing the query:", e)

    return row_number

def cores_number_obtainer():
    cpu_number = multiprocessing.cpu_count()
    return cpu_number

def pagination_creator(row_number, cpu_number):
    limit = math.ceil(row_number/cpu_number)
    return limit

def paginate_query(base_query, cpu_number, limit, mapping_graph, base_tm, row, pred):
    for i in range(cpu_number):
        offset = limit * i
        paginated_query = f"{base_query} LIMIT {limit} OFFSET {offset}"

        # TriplesMap con sufijo _pageX
        tm_uri = URIRef(f"#{base_tm}_page{i+1}")

        # LogicalSource
        ls_bnode = BNode()
        mapping_graph.add((tm_uri, RML.logicalSource, ls_bnode))
        mapping_graph.add((ls_bnode, RML.query, Literal(paginated_query)))
        mapping_graph.add((ls_bnode, RML.referenceFormulation, URIRef(RML.SQL2008)))

        # SubjectMap
        sm_bnode = BNode()
        mapping_graph.add((tm_uri, RR.subjectMap, sm_bnode))
        mapping_graph.add((sm_bnode, RR.template, Literal(f"{base_tm}_{i+1}")))

        # PredicateObjectMap
        if row.pom:
            pom_bnode = BNode()
            om_bnode = BNode()
            mapping_graph.add((tm_uri, RML.predicateObjectMap, pom_bnode))
            if pred:
                mapping_graph.add((pom_bnode, RML.predicate, URIRef(pred)))
            mapping_graph.add((pom_bnode, RML.objectMap, om_bnode))

    return mapping_graph


def process_query(query, conn, mapping_graph, base_tm, row, pred):
    """ Check the query and decide if paginate """
    row_number = row_counter(conn, query)
    print(f"The query {base_query} has {row_number} rows.")

    if row_number > 10000:
        cpu_number = cores_number_obtainer()
        limit = pagination_creator(row_number, cpu_number)
        print(f"Paginate {base_tm} on {cpu_number} cores with LIMIT {limit}")
        mapping_graph = paginate_query(query, cpu_number, limit, mapping_graph, base_tm, row, pred)
    else:
        # Sin paginación → una sola entrada
        tm_uri = URIRef(f"#{base_tm}")
        ls_bnode = BNode()
        mapping_graph.add((tm_uri, RML.logicalSource, ls_bnode))
        mapping_graph.add((ls_bnode, RML.query, Literal(query)))
    return mapping_graph

for row in results:
    base_tm = str(row.tm).split("#")[-1]
    base_query = str(row.query).replace("\r", "")
    pred = str(row.pom) if row.pom else None
    mapping_graph = process_query(base_query, conn, mapping_graph, base_tm, row, pred)

# === Save output to TTL ===
output_path = "paginated_mapping.ttl"
mapping_graph.serialize(destination=output_path, format='turtle')
print(f"\n✅ Mapping saved successfully to {output_path}")