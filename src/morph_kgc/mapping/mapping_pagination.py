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

def row_counter(db_url: str, query: str) -> int:
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")
    try:
        conn_local = psycopg.connect(db_url)
        cur = conn_local.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({query}) AS subquery")
        total = cur.fetchone()[0]
        cur.close()
        conn_local.close()
        return total
    except Exception as e:
        return 0

def cores_number_obtainer() -> int:
    return multiprocessing.cpu_count()

def pagination_creator(total_rows: int, cpu_number: int) -> int:
    return math.ceil(total_rows / cpu_number)

def copy_recursive_with_new_bnodes(graph_in, graph_out, subject, bnode_map):
    if isinstance(subject, BNode):
        if subject in bnode_map:
            return bnode_map[subject]
        new_subject = BNode()
        bnode_map[subject] = new_subject
    else:
        new_subject = subject

    for s, p, o in graph_in.triples((subject, None, None)):
        new_s = bnode_map[s] if isinstance(s, BNode) and s in bnode_map else s
        if isinstance(o, BNode):
            new_o = copy_recursive_with_new_bnodes(graph_in, graph_out, o, bnode_map)
        else:
            new_o = o
        graph_out.add((new_s, p, new_o))

    return new_subject

def generate_query_from_table_name(table_name: str) -> str:
    table_str = table_name.strip()
    if '.' in table_str:
        parts = table_str.split('.')
        parts_escaped = [f'"{p.strip().replace("\"","\"\"")}"' for p in parts]
        table_escaped = '.'.join(parts_escaped)
    else:
        table_escaped = f'"{table_str.replace("\"","\"\"")}"'
    return f'SELECT * FROM {table_escaped}'

def pagination(input_path: str, output_path: str, config_path: str = "default_config.ini"):
    
    g = Graph()
    g.parse(input_path, format='turtle')

    # Configuración DB
    config = configparser.ConfigParser()
    config.read(config_path)
    conn = config.get("DataSource1", "db_url")

    # Grafo de salida
    mapping_graph = Graph()
    for prefix, ns in g.namespaces():
        mapping_graph.bind(prefix, ns)

    uris_to_copy = set()
    paginated_tms = set()

    for tm in g.subjects(RML.logicalSource, None):
        logical_source = next(g.objects(tm, RML.logicalSource), None)
        sm = next(g.objects(tm, RML.subjectMap), None)
        pom = next(g.objects(tm, RML.predicateObjectMap), None)

        # Obtener query
        query_obj = next(g.objects(logical_source, RML.query), None)
        table_obj = next(g.objects(logical_source, RML.tableName), None)

        if query_obj:
            sql_query = str(query_obj)
        elif table_obj:
            sql_query = generate_query_from_table_name(str(table_obj))
        else:
            uris_to_copy.add(tm)
            if sm: uris_to_copy.add(sm)
            if pom: uris_to_copy.add(pom)
            continue

        total_rows = row_counter(conn, sql_query)

        if total_rows > 10000:
            cpu_number = cores_number_obtainer()
            limit = pagination_creator(total_rows, cpu_number)
            paginated_tms.add(tm)

            for i in range(cpu_number):
                offset = i * limit
                if offset + limit >= total_rows:
                    paginated_query = f"{sql_query} OFFSET {offset}"
                else:
                    paginated_query = f"{sql_query} LIMIT {limit} OFFSET {offset}"

                new_tm_uri = URIRef(str(tm) + f"_Page{i+1}")
                new_ls = BNode()

                for p, o in g.predicate_objects(logical_source):
                    if p != RML.query:
                        mapping_graph.add((new_ls, p, o))

                mapping_graph.add((new_ls, RML.query, Literal(paginated_query)))
                mapping_graph.add((new_tm_uri, RML.logicalSource, new_ls))

                bnode_map_page = {}
                if sm:
                    new_sm = copy_recursive_with_new_bnodes(g, mapping_graph, sm, bnode_map_page)
                    mapping_graph.add((new_tm_uri, RML.subjectMap, new_sm))
                if pom:
                    new_pom = copy_recursive_with_new_bnodes(g, mapping_graph, pom, bnode_map_page)
                    mapping_graph.add((new_tm_uri, RML.predicateObjectMap, new_pom))

        else:
            uris_to_copy.add(tm)
            if sm: uris_to_copy.add(sm)
            if pom: uris_to_copy.add(pom)

    bnode_map_global = {}
    for uri in uris_to_copy:
        if uri not in paginated_tms:
            copy_recursive_with_new_bnodes(g, mapping_graph, uri, bnode_map_global)
    mapping_graph.serialize(destination=output_path, format='turtle')
    print(f"Mapping generated in: {output_path}")

