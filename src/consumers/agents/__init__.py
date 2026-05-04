# Importing each module causes the @app.agent / @app.task decorators to fire,
# registering every agent and task with the Faust app instance.
from agents import process, reader, eviction, db_writer

__all__ = ["process", "reader", "eviction", "db_writer"]