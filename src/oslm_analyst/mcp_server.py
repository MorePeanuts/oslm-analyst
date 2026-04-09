"""
OSIR-LMTS Database MCP Server

A Model Context Protocol server that provides safe read-only access
to the osir_lmts.db database.
"""

import asyncio
import sqlite3
import json
import os
from typing import Any
from contextlib import contextmanager

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent


app = Server('osir-lmts-db')

# Database path - will be set by main()
_db_path: str = 'output/osir_lmts.db'


def set_db_path(db_path: str):
    """Set the database path."""
    global _db_path
    _db_path = db_path


@contextmanager
def get_db_connection():
    """Get a database connection with read-only access."""
    # If path is relative, try to resolve it properly
    if not os.path.isabs(_db_path):
        # Try to find project root - look for pyproject.toml
        project_root = None
        current_dir = os.path.dirname(os.path.abspath(__file__))
        for _ in range(5):
            if os.path.exists(os.path.join(current_dir, 'pyproject.toml')):
                project_root = current_dir
                break
            parent = os.path.dirname(current_dir)
            if parent == current_dir:
                break
            current_dir = parent

        if project_root is None:
            # Fallback to current working directory
            db_path = _db_path
        else:
            db_path = os.path.join(project_root, _db_path)
    else:
        db_path = _db_path

    if not os.path.exists(db_path):
        raise FileNotFoundError(f'Database not found at: {db_path}')

    # Open in read-only mode
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def is_read_only_query(sql: str) -> bool:
    """Check if the SQL query is read-only (SELECT only)."""
    normalized_sql = sql.strip().upper()
    return normalized_sql.startswith('SELECT') or normalized_sql.startswith('WITH')


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name='query_osir_lmts_db',
            description='Query the osir_lmts.db database with read-only SQL. '
            'Use this for querying model and dataset data, '
            'download statistics, organization rankings, etc.',
            inputSchema={
                'type': 'object',
                'properties': {
                    'sql': {
                        'type': 'string',
                        'description': 'SQL query to execute (SELECT or WITH only)',
                    },
                },
                'required': ['sql'],
            },
        ),
        Tool(
            name='get_db_schema',
            description='Get the database schema information including tables and columns',
            inputSchema={
                'type': 'object',
                'properties': {},
            },
        ),
        Tool(
            name='get_available_months',
            description='Get list of available months in the database',
            inputSchema={
                'type': 'object',
                'properties': {},
            },
        ),
        Tool(
            name='get_latest_month',
            description='Get the latest month available in the database',
            inputSchema={
                'type': 'object',
                'properties': {},
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == 'query_osir_lmts_db':
            return await handle_query(arguments)
        elif name == 'get_db_schema':
            return await handle_get_schema()
        elif name == 'get_available_months':
            return await handle_get_available_months()
        elif name == 'get_latest_month':
            return await handle_get_latest_month()
        else:
            return [TextContent(type='text', text=f'Unknown tool: {name}')]
    except Exception as e:
        return [TextContent(type='text', text=f'Error: {str(e)}')]


async def handle_query(arguments: dict[str, Any]) -> list[TextContent]:
    """Handle database query."""
    sql = arguments['sql']

    if not is_read_only_query(sql):
        return [
            TextContent(
                type='text', text='Error: Only SELECT or WITH queries are allowed for safety.'
            )
        ]

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch all rows
            rows = cursor.fetchall()

            if not rows:
                return [
                    TextContent(
                        type='text',
                        text=json.dumps(
                            {
                                'columns': columns,
                                'rows': [],
                                'row_count': 0,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                    )
                ]

            # Convert rows to list of dicts
            result_rows = []
            for row in rows:
                result_rows.append(dict(row))

            # Limit result size for large datasets
            max_rows = 1000
            if len(result_rows) > max_rows:
                result_rows = result_rows[:max_rows]
                note = f' (showing first {max_rows} of {len(rows)} rows)'
            else:
                note = ''

            return [
                TextContent(
                    type='text',
                    text=json.dumps(
                        {
                            'columns': columns,
                            'rows': result_rows,
                            'row_count': len(rows),
                            'note': note,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                )
            ]

    except sqlite3.Error as e:
        return [TextContent(type='text', text=f'Database error: {str(e)}')]


async def handle_get_schema() -> list[TextContent]:
    """Get database schema."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        schema = {}
        for table in tables:
            cursor.execute(f'PRAGMA table_info({table})')
            columns = cursor.fetchall()
            schema[table] = [
                {
                    'name': col[1],
                    'type': col[2],
                    'not_null': bool(col[3]),
                    'default_value': col[4],
                    'primary_key': bool(col[5]),
                }
                for col in columns
            ]

        return [TextContent(type='text', text=json.dumps(schema, ensure_ascii=False, indent=2))]


async def handle_get_available_months() -> list[TextContent]:
    """Get available months."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT month FROM models ORDER BY month')
        months = [row[0] for row in cursor.fetchall()]

        return [
            TextContent(
                type='text', text=json.dumps({'months': months}, ensure_ascii=False, indent=2)
            )
        ]


async def handle_get_latest_month() -> list[TextContent]:
    """Get latest month."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(month) as max_month FROM models')
        result = cursor.fetchone()

        return [
            TextContent(
                type='text',
                text=json.dumps(
                    {'latest_month': result[0] if result else None}, ensure_ascii=False, indent=2
                ),
            )
        ]


async def main(db_path: str = 'output/osir_lmts.db'):
    """Run the MCP server."""
    set_db_path(db_path)
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

