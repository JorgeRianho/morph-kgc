from rdflib import Graph, Namespace, URIRef, BNode, Literal
import multiprocessing
import math
import psycopg
import configparser

# ==========================
# Namespaces
# ==========================
RML = Namespace("http://w3id.org/rml/")
RR  = Namespace("http://www.w3.org/ns/r2rml#")
UB  = Namespace("http://swat.cse.lehigh.edu/onto/univ-bench.owl#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

# Cargar grafo
g = Graph()
g.parse('/home/jorge/proyectos/git/morph-kgc/examples/configuration-file/normalized_mapping.ttl', format='turtle')
    
# Configuración de DB
config = configparser.ConfigParser()
config.read("default_config.ini")
conn = config.get("DataSource1", "db_url")

# SPARQL
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
mapping_graph = Graph()

# ==========================
# Contadores y utilidades
# ==========================
def row_counter(db_url: str, query: str) -> int:
    """Devuelve el número de filas de una consulta SQL."""
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    try:
        conn_local = psycopg.connect(db_url)
        cur = conn_local.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subquery")
        total = cur.fetchone()[0]
        cur.close()
        conn_local.close()
        print(f"🧮 Filas totales: {total}")
        return total
    except Exception as e:
        print("❌ Error al contar filas:", e)
        return 0

def cores_number_obtainer() -> int:
    """Devuelve el número de núcleos disponibles."""
    cpu_number = multiprocessing.cpu_count()
    return cpu_number

def pagination_creator(total_rows: int, cpu_number: int) -> int:
    """Calcula el límite por página según filas y núcleos."""
    limit = math.ceil(total_rows / cpu_number)
    return limit

# ==========================
# Función para copiar subgrafos
# ==========================
def copy_recursive(graph_in, graph_out, subject, visited=None):
    """Copia recursivamente todos los triples accesibles desde 'subject'."""
    if visited is None:
        visited = set()
    if subject in visited:
        return
    visited.add(subject)

    for s, p, o in graph_in.triples((subject, None, None)):
        graph_out.add((s, p, o))
        if isinstance(o, (URIRef, BNode)):
            copy_recursive(graph_in, graph_out, o, visited)

# ==========================
# Función principal
# ==========================
def copy_mapping_with_query(input_path: str, output_path: str):
    
    for prefix, ns in g.namespaces():
        mapping_graph.bind(prefix, ns)
    

    
    uris_to_copy = set()
    paginated_tms = set()
    
    for row in results:
        tm, query, sm, pom = row
        if query and isinstance(query, Literal):
            sql_query = str(query)
            total_rows = row_counter(conn, sql_query)
            
            if total_rows > 10000:
                cpu_number = cores_number_obtainer()
                limit = pagination_creator(total_rows, cpu_number)
                logical_source_original = next(g.objects(tm, RML.logicalSource), None)
                if not logical_source_original:
                    continue
                
                paginated_tms.add(tm)
                
                for i in range(cpu_number):
                    offset = i * limit
                    # Última página sin limit si excede
                    if offset + limit >= total_rows:
                        paginated_query = f"{sql_query} OFFSET {offset}"
                    else:
                        paginated_query = f"{sql_query} LIMIT {limit} OFFSET {offset}"
                    
                    new_tm_uri = URIRef(str(tm) + f"_Page{i+1}")
                    new_ls = BNode()
                    
                    # Copiar propiedades excepto query
                    for p, o in g.predicate_objects(logical_source_original):
                        if p != RML.query:
                            mapping_graph.add((new_ls, p, o))
                    
                    mapping_graph.add((new_ls, RML.query, Literal(paginated_query)))
                    mapping_graph.add((new_tm_uri, RML.logicalSource, new_ls))
                    
                    if sm:
                        mapping_graph.add((new_tm_uri, RML.subjectMap, sm))
                        if isinstance(sm, BNode):
                            copy_recursive(g, mapping_graph, sm)
                    if pom:
                        mapping_graph.add((new_tm_uri, RML.predicateObjectMap, pom))
                        if isinstance(pom, BNode):
                            copy_recursive(g, mapping_graph, pom)
                    
                    print(f"🧩 Creado mapping paginado: {new_tm_uri}")
            else:
                uris_to_copy.add(tm)
                if sm: uris_to_copy.add(sm)
                if pom: uris_to_copy.add(pom)
        else:
            uris_to_copy.add(tm)
            if sm: uris_to_copy.add(sm)
            if pom: uris_to_copy.add(pom)
    
    # Copiar TMs no paginados
    for uri in uris_to_copy:
        if uri not in paginated_tms:
            copy_recursive(g, mapping_graph, uri)
    
    mapping_graph.serialize(destination=output_path, format='turtle')
    print(f"✅ Mapping final guardado en: {output_path}")

# ==========================
# Ejemplo de uso
# ==========================
if __name__ == "__main__":
    copy_mapping_with_query("normalized_mapping.ttl", "output_mapping_paginated.ttl")
