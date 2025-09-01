from dag.channel import Channel
from context.context import QueryContext, InvalidContext

class ContextManager:
    _sql_channel: Channel
    
    def register_channel(self, channel: Channel):
        if not hasattr(self, "_sql_channel"):
            self._sql_channel = channel
        else:
            raise Exception("Attempt to submit sql channel to ContextManager, but one already exists")
        
    
    def parse(self, sql: str) -> QueryContext | InvalidContext:
        pass
