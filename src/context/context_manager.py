from commons.utils import Channel
from context.context import QueryContext, InvalidContext
from sql.sqlparser.parser import extract_one_query_context

class ContextManager:
    
    @staticmethod
    def parse(sql: str, properties_schema: dict) -> QueryContext | InvalidContext:
        return extract_one_query_context(sql, properties_schema)
