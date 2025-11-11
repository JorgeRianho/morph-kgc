from rdflib import Graph, Namespace, URIRef, BNode, Literal
import os
import math
import rdflib
import pandas as pd
import multiprocessing
import psycopg
import configparser

# ==========================
# Namespaces
# ==========================
RML = Namespace("http://w3id.org/rml/")
RR  = Namespace("http://www.w3.org/ns/r2rml#")
UB  = Namespace("http://swat.cse.lehigh.edu/onto/univ-bench.owl#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

# ==========================
# Carga del grafo y config
# ==========================
g = rdflib.Graph()
g.parse('/home/jorge/proyectos/git/morph-kgc/examples/configuration-file/normalized_mapping.ttl', format='turtle')

config = configparser.ConfigParser()
config.read("default_config.ini")
conn = config.get("DataSource1", "db_url")

# ==========================
# SPARQL para extraer mappings
# ==========================
q_query = """
    PREFIX rml: <http://w3id.org/rml/>
    PREFIX rr: <http://www.w3.org/ns/r2rml#>
    PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>

    SELECT DISTINCT ?tm ?query ?sm ?pom WHERE {
        ?tm rml:logicalSource ?ls .
        ?ls rml:query ?query .
        ?tm rml:subjectMap ?sm .
        OPTIONAL { ?tm rml:predicateObjectMap ?pom . }
    }
    """

results = g.query(q_query)

# Grafo de salida
mapping_graph = rdflib.Graph()
mapping_graph.bind("rml", RML)
mapping_graph.bind("rr", RR)
mapping_graph.bind("ub", UB)


# ==============================
# === FUNCIONES AUXILIARES ===
# ==============================

def row_counter(db_url: str, query: str):
    """
    Devuelve el número total de filas que genera una consulta SQL.
    """
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")
    try:
        conn_local = psycopg.connect(db_url)
        cur = conn_local.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subquery")
        row_number = cur.fetchone()[0]
        cur.close()
        conn_local.close()
        print(f"🧮 Filas totales: {row_number}")
        return row_number
    except Exception as e:
        print("❌ Error al contar filas:", e)
        return 0


def cores_number_obtainer():
    """
    Devuelve el número de núcleos disponibles.
    """
    cpu_number = multiprocessing.cpu_count()
    print(f"⚙️ Núcleos detectados: {cpu_number}")
    return cpu_number


def pagination_creator(row_number, cpu_number):
    """
    Calcula el límite por página según filas y núcleos.
    """
    limit = math.ceil(row_number / cpu_number)
    print(f"📐 Límite por página calculado: {limit}")
    return limit


# =======================================
# === FUNCIÓN PRINCIPAL DE PROCESADO ===
# =======================================
def copy_mapping_with_query(input_path, output_path):
    """
    Copia el mapping RDF aplicando paginación si la query tiene más de 1000 filas.
    """

    # === Copia recursiva (declarada primero para poder usarse después) ===
    def copy_recursive(subject, visited=None):
        """
        Copia al mapping_graph todo el subgrafo accesible desde 'subject',
        siguiendo objetos que sean URIRef o BNode.
        """
        if visited is None:
            visited = set()
        if subject in visited:
            return
        visited.add(subject)

        for s, p, o in g.triples((subject, None, None)):
            mapping_graph.add((s, p, o))
            if isinstance(o, (URIRef, BNode)):
                copy_recursive(o, visited)

    # Función auxiliar para copiar subgrafos (usada al crear paginados para SM/POM BNodes)
    def _copy_subgraph(start_node):
        copy_recursive(start_node, visited=set())

    uris_to_copy = set()
    paginated_original_tms = set()

    for row in results:
        tm, query, sm, pom = row
        if query and isinstance(query, Literal):
            sql_query = str(query)
            total_rows = row_counter(conn, sql_query)

            if total_rows > 1000:
                cpu_number = cores_number_obtainer()
                limit = pagination_creator(total_rows, cpu_number)
                print(f"⚙️ Aplicando paginación a {tm}: {cpu_number} páginas aprox., limit={limit}")

                logical_source_original = next(g.objects(tm, RML.logicalSource), None)
                if logical_source_original is None:
                    print(f"⚠️ No se encontró rml:logicalSource para {tm}, se omite paginación.")
                    continue

                paginated_original_tms.add(tm)

                for i in range(cpu_number):
                    offset = i * limit
                    paginated_query = f"{sql_query} LIMIT {limit} OFFSET {offset}"

                    new_tm_uri = URIRef(str(tm) + f"_page{i+1}")
                    new_ls = BNode()

                    # Copiar propiedades del logicalSource original, excepto rml:query
                    for p, o in g.predicate_objects(logical_source_original):
                        if p != RML.query:  # Evita duplicar la query
                            mapping_graph.add((new_ls, p, o))

                    # Añadir solo la query paginada
                    mapping_graph.add((new_ls, RML.query, Literal(paginated_query)))


                    # Asociar elementos
                    mapping_graph.add((new_tm_uri, RML.logicalSource, new_ls))
                    if sm is not None:
                        mapping_graph.add((new_tm_uri, RML.subjectMap, sm))
                        if isinstance(sm, BNode):
                            _copy_subgraph(sm)
                    if pom is not None:
                        mapping_graph.add((new_tm_uri, RML.predicateObjectMap, pom))
                        if isinstance(pom, BNode):
                            _copy_subgraph(pom)

                    print(f"🧩 Creado mapping paginado: {new_tm_uri}")
            else:
                for v in row:
                    if v is not None:
                        uris_to_copy.add(v)
        else:
            for v in row:
                if v is not None:
                    uris_to_copy.add(v)

    uris_to_copy = {u for u in uris_to_copy if isinstance(u, (URIRef, BNode))}
    uris_to_copy = {u for u in uris_to_copy if u not in paginated_original_tms}

    print(f"🔗 URIs encontradas para copiar sin paginar: {len(uris_to_copy)}")

    for uri in uris_to_copy:
        copy_recursive(uri)

    mapping_graph.serialize(destination=output_path, format="turtle")
    print(f"✅ Mapping copiado y paginado guardado en: {output_path}")



# === Programa principal ===
if __name__ == "__main__":
    input_file = "normalized_mapping.ttl"
    output_file = "output_mapping_paginated.ttl"
    copy_mapping_with_query(input_file, output_file)
