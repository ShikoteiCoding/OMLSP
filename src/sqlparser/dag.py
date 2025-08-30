import networkx as nx # to replace by owned one ?
import sqlglot
from sqlglot.expressions import Table, Insert, Create, With, TableAlias, Select

def find_tables_in_expression(expression):
    """Recursively finds all table/view references in a sqlglot expression."""
    tables = set()
    # Use find_all(Table) to capture all tables, views, etc.
    for sub_expression in expression.find_all(Table):
        tables.add(sub_expression.name)
    return tables

def build_sql_lineage_dag(sql_statements, dialect=None):
    """
    Parses SQL statements and builds a table/view/materialized view dependency DAG.

    Args:
        sql_statements (str): A string of one or more SQL statements,
                              separated by semicolons.
        dialect (str, optional): The SQL dialect (e.g., 'snowflake', 'hive').

    Returns:
        networkx.DiGraph: The dependency graph.
    """
    dag = nx.DiGraph()
    parsed_statements = sqlglot.parse(sql_statements, read=dialect)
    
    # Map for handling CTEs, as their scope is local to a statement
    cte_map = {}

    if not parsed_statements:
        return dag

    for expression in parsed_statements:
        target_object = None
        source_objects = set()

        if not expression:
            continue

        # Find the target object from a CREATE statement
        create_expression = expression.find(Create)
        if create_expression:
            target_object = create_expression.this.name
            
            # Find the subquery that defines the object's contents
            query_expression = create_expression.find(Select)
            if query_expression:
                source_objects = find_tables_in_expression(query_expression)

                for source in source_objects:
                    # Resolve dependencies, including CTEs
                    if source in cte_map:
                        for cte_source_parent in cte_map[source]:
                            dag.add_edge(cte_source_parent, target_object)
                    else:
                        dag.add_edge(source, target_object)
        
        # Handle INSERT statements
        insert_expression = expression.find(Insert)
        if insert_expression:
            target_object = insert_expression.this.name
            query_expression = insert_expression.find(Select)
            if query_expression:
                source_objects = find_tables_in_expression(query_expression)
                for source in source_objects:
                     dag.add_edge(source, target_object)

    return dag


if __name__ == "__main__":
    sql_query = """
    -- Create a base table
    CREATE TABLE raw_customers AS
    SELECT id, name, created_at FROM external_data.customers;

    -- Create a view on the base table
    CREATE VIEW active_customers AS
    SELECT * FROM raw_customers WHERE is_active = TRUE;

    -- Create a materialized view on the base table
    CREATE MATERIALIZED VIEW mv_customer_summary AS
    SELECT
        date(created_at) AS creation_date,
        count(*) AS total_customers
    FROM raw_customers
    GROUP BY creation_date;

    -- Create another view, based on an existing view
    CREATE VIEW recent_active_customers AS
    SELECT * FROM active_customers WHERE created_at >= '2025-01-01';

    -- Create a table combining the materialized view and another table
    CREATE TABLE monthly_report AS
    SELECT
        s.creation_date,
        s.total_customers,
        o.total_orders
    FROM mv_customer_summary AS s
    JOIN orders AS o
    ON s.creation_date = date(o.order_date);
    """
    graph = build_sql_lineage_dag(sql_query)