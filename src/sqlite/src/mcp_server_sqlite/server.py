import os
import sys
import sqlite3
import logging
from contextlib import closing
from pathlib import Path
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse
import json

logger = logging.getLogger('mcp_sqlite_server')
logging.basicConfig(level=logging.INFO)

class SqliteDatabase:
    def __init__(self, db_path: str):
        self.db_path = str(Path(db_path).expanduser())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
        self.insights = []

    def _init_database(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row

    def _execute_query(self, query: str):
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            with closing(conn.cursor()) as cursor:
                cursor.execute(query)
                if query.upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE')):
                    conn.commit()
                    return [{"affected_rows": cursor.rowcount}]
                return [dict(row) for row in cursor.fetchall()]

db = None

async def mcp_endpoint(request):
    data = await request.json()
    tool = data.get("tool")
    args = data.get("arguments", {})
    try:
        if tool == "list_tables":
            result = db._execute_query("SELECT name FROM sqlite_master WHERE type='table'")
        elif tool == "read_query":
            result = db._execute_query(args["query"])
        else:
            return PlainTextResponse("Unknown tool", status_code=400)
        return PlainTextResponse(json.dumps(result))
    except Exception as e:
        return PlainTextResponse(f"Error: {str(e)}", status_code=400)

app = Starlette(routes=[Route("/mcp", mcp_endpoint, methods=["POST"])])

async def main(db_path: str):
    global db
    db = SqliteDatabase(db_path)
    logger.info(f"Starting SQLite MCP Server with DB path: {db_path}")
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    args = parser.parse_args()
    import asyncio
    asyncio.run(main(args.db_path))