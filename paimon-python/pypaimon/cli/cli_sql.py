#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Interactive SQL shell for querying Paimon tables via DuckDB."""

import atexit
import os
import sys


def add_sql_subcommand(parser):
    """Add arguments to the ``sql`` subparser."""
    parser.add_argument(
        'query', nargs='?', default=None,
        help='SQL query to execute (non-interactive mode)'
    )
    parser.add_argument(
        '--database', '-d', default=None,
        help='Default Paimon database name'
    )
    parser.set_defaults(func=cmd_sql)


def _execute_and_print(db, sql):
    """Execute a SQL statement via PaimonDuckDB and print the result."""
    result = db.sql(sql)
    try:
        df = result.fetchdf()
        if not df.empty:
            print(df.to_string(index=False))
    except Exception:
        # DDL or statements with no result set
        pass


def cmd_sql(args):
    """Entry point for the ``paimon sql`` command."""
    try:
        import duckdb  # noqa: F401
    except ImportError:
        print(
            "Error: duckdb is required for the sql command.\n"
            "Install it with: pip install 'ks-pypaimon[duckdb]'",
            file=sys.stderr,
        )
        sys.exit(1)

    from pypaimon.cli.cli import load_catalog_config
    config = load_catalog_config(args.config)

    database = args.database or config.pop('database', None) or 'default'

    from pypaimon.duckdb import PaimonDuckDB
    db = PaimonDuckDB(config, database=database)

    if args.query is not None:
        # Non-interactive: single query from positional argument
        _execute_and_print(db, args.query)
        return

    if not sys.stdin.isatty():
        # Piped input
        content = sys.stdin.read()
        for stmt in content.split(';'):
            stmt = stmt.strip()
            if stmt:
                _execute_and_print(db, stmt)
        return

    # Interactive REPL
    repl = PaimonSqlRepl(db, config, database)
    repl.run()


class PaimonSqlRepl:
    """Interactive SQL REPL backed by PaimonDuckDB."""

    PROMPT = 'paimon> '
    CONTINUATION = '     -> '

    def __init__(self, db, catalog_options, database):
        self.db = db
        self.catalog_options = catalog_options
        self.database = database
        self._catalog = None

    @property
    def catalog(self):
        if self._catalog is None:
            from pypaimon.cli.cli import create_catalog
            self._catalog = create_catalog(self.catalog_options)
        return self._catalog

    def run(self):
        self._setup_readline()
        print("Apache Paimon SQL Shell (DuckDB backend)")
        print("Database: {}".format(self.database))
        print("Type .help for help, .quit to exit.")
        print()

        while True:
            try:
                stmt = self._read_statement()
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                continue

            if stmt is None:
                continue

            if stmt.startswith('.'):
                should_exit = self._handle_dot_command(stmt)
                if should_exit:
                    break
            else:
                try:
                    _execute_and_print(self.db, stmt)
                except Exception as e:
                    print("Error: {}".format(e), file=sys.stderr)

    def _read_statement(self):
        """Read a (possibly multi-line) SQL statement ending with ``;``."""
        first_line = input(self.PROMPT).strip()
        if not first_line:
            return None

        # Dot commands are always single-line
        if first_line.startswith('.'):
            return first_line

        buf = first_line
        while not buf.rstrip().endswith(';'):
            try:
                line = input(self.CONTINUATION)
            except EOFError:
                break
            buf += '\n' + line

        # Strip trailing semicolon for DuckDB execution
        return buf.rstrip().rstrip(';').strip()

    def _handle_dot_command(self, line):
        """Handle dot-commands. Returns True if the REPL should exit."""
        parts = line.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ''

        if cmd in ('.quit', '.exit'):
            return True

        if cmd == '.help':
            self._print_help()
        elif cmd == '.tables':
            self._show_tables()
        elif cmd == '.schema':
            if not arg:
                print("Usage: .schema <table_name>", file=sys.stderr)
            else:
                self._show_schema(arg)
        elif cmd == '.databases':
            self._show_databases()
        elif cmd == '.use':
            if not arg:
                print("Usage: .use <database_name>", file=sys.stderr)
            else:
                self._switch_database(arg)
        else:
            print("Unknown command: {}. Type .help for help.".format(cmd),
                  file=sys.stderr)
        return False

    def _print_help(self):
        print("Commands:")
        print("  .tables              List tables in current database")
        print("  .schema <table>      Show table schema")
        print("  .databases           List all databases")
        print("  .use <database>      Switch database")
        print("  .help                Show this help")
        print("  .quit / .exit        Exit the shell")
        print()
        print("SQL queries end with ';' and support multi-line input.")
        print("Time travel: SELECT * FROM t VERSION AS OF 42;")

    def _show_tables(self):
        try:
            tables = self.catalog.list_tables(self.database)
            for t in tables:
                print(t)
        except Exception as e:
            print("Error: {}".format(e), file=sys.stderr)

    def _show_schema(self, table_name):
        try:
            identifier = '{}.{}'.format(self.database, table_name)
            table = self.catalog.get_table(identifier)
            fields = table.table_schema.fields
            if not fields:
                print("(no columns)")
                return
            max_name = max(len(f.name) for f in fields)
            header_name = 'Column'
            header_type = 'Type'
            max_name = max(max_name, len(header_name))
            print('{}  {}'.format(header_name.ljust(max_name), header_type))
            print('{}  {}'.format('-' * max_name, '-' * 20))
            for f in fields:
                print('{}  {}'.format(f.name.ljust(max_name), str(f.type)))
        except Exception as e:
            print("Error: {}".format(e), file=sys.stderr)

    def _show_databases(self):
        try:
            databases = self.catalog.list_databases()
            for db in databases:
                print(db)
        except Exception as e:
            print("Error: {}".format(e), file=sys.stderr)

    def _switch_database(self, database):
        self.database = database
        self.db.database = database
        print("Switched to database: {}".format(database))

    def _setup_readline(self):
        try:
            import readline
            history_path = os.path.expanduser('~/.paimon_sql_history')
            try:
                readline.read_history_file(history_path)
            except (FileNotFoundError, IOError):
                pass
            readline.set_history_length(1000)
            atexit.register(readline.write_history_file, history_path)
        except (ImportError, Exception):
            pass
