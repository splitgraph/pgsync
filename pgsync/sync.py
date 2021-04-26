# -*- coding: utf-8 -*-

"""Main module."""
import collections
import itertools
import json
import logging
import os
import pprint
import re
import select
import sys
import time
from datetime import datetime, timedelta

import click
import psycopg2
import sqlalchemy as sa

from . import __version__
from .base import Base, compiled_query
from .constants import (
    DELETE,
    INSERT,
    META,
    PRIMARY_KEY_DELIMITER,
    SCHEMA,
    TG_OP,
    TRUNCATE,
    UPDATE,
)
from .elastichelper import ElasticHelper
from .exc import RDSError, SuperUserError
from .node import traverse_breadth_first, traverse_post_order, Tree
from .plugin import Plugins
from .querybuilder import QueryBuilder
from .redisqueue import RedisQueue
from .settings import (
    POLL_TIMEOUT,
    REDIS_POLL_INTERVAL,
    REPLICATION_SLOT_CLEANUP_INTERVAL, ELASTICSEARCH_CHUNK_SIZE, QUERY_CHUNK_SIZE,
)
from .transform import get_private_keys, transform
from .utils import get_config, progress, show_settings, threaded, Timer

logger = logging.getLogger(__name__)


class Sync(Base):
    """Main application class for Sync."""

    def __init__(
        self,
        document,
        verbose=False,
        params=None,
        validate=True,
    ):
        """Constructor."""
        params = params or {}
        self.index = document['index']
        self.pipeline = document.get('pipeline')
        self.plugins = document.get('plugins', [])
        self.nodes = document.get('nodes', [])
        self.setting = document.get('setting')
        super().__init__(document.get('database', self.index), **params)
        self.es = ElasticHelper()
        self.__name = re.sub(
            '[^0-9a-zA-Z_]+', '', f"{self.database}_{self.index}"
        )
        self._checkpoint = None
        self._plugins = None
        self._truncate = False
        self.verbose = verbose
        self._checkpoint_file = f".{self.__name}"
        self.tree = Tree(self)
        self._last_truncate_timestamp = datetime.now()
        if validate:
            self.validate()
            self.create_setting()
        self.query_builder = QueryBuilder(self, verbose=self.verbose)

    def validate(self):
        """Perform all validation right away."""
        self.connect()
        if self.plugins:
            self._plugins = Plugins('plugins', self.plugins)

        max_replication_slots = self.pg_settings('max_replication_slots')
        try:
            if int(max_replication_slots) < 1:
                raise TypeError
        except TypeError:
            raise RuntimeError(
                'Ensure there is at least one replication slot defined '
                'by setting max_replication_slots=1'
            )

        wal_level = self.pg_settings('wal_level')
        if not wal_level or wal_level.lower() != 'logical':
            raise RuntimeError(
                'Enable logical decoding by setting wal_level=logical'
            )

        rds_logical_replication = self.pg_settings('rds.logical_replication')

        if rds_logical_replication:
            if rds_logical_replication.lower() == 'off':
                raise RDSError('rds.logical_replication is not enabled')
        else:
            if not (
                self.has_permission(
                    self.engine.url.username,
                    'usesuper',
                ) or self.has_permission(
                    self.engine.url.username,
                    'userepl',
                )
            ):
                raise SuperUserError(
                    f'PG_USER "{self.engine.url.username}" needs to be '
                    f'superuser or have replication role permission to '
                    f'perform this action. '
                    f'Ensure usesuper or userepl is True in pg_user'
                )

        if self.index is None:
            raise ValueError('Index is missing for document')

        root = self.tree.build(self.nodes[0])
        root.display()
        for node in traverse_breadth_first(root):
            pass

    def create_setting(self):
        """Create Elasticsearch setting and mapping if required."""
        root = self.tree.build(self.nodes[0])
        self.es._create_setting(self.index, root, setting=self.setting)

    def setup(self):
        """Create the database triggers and replication slot."""
        self.teardown(drop_views=False)
        self.create_replication_slot(self.__name)

    def teardown(self, drop_views=True):
        """Drop the database triggers and replication slot."""

        try:
            os.unlink(self._checkpoint_file)
        except OSError:
            pass

        for schema in self.schemas:
            tables = set([])
            root = self.tree.build(self.nodes[0])
            for node in traverse_breadth_first(root):
                tables |= set(node.relationship.through_tables)
                tables |= set([node.table])
            self.drop_triggers(schema=schema, tables=tables)
            if drop_views:
                self.drop_views(schema=schema)
        self.drop_replication_slot(self.__name)

    def get_doc_id(self, primary_keys):
        """Get the Elasticsearch document id from the primary keys."""
        return f'{PRIMARY_KEY_DELIMITER}'.join(
            map(str, primary_keys)
        )

    def logical_slot_changes(self, txmin=None, txmax=None):
        """
        Process changes from the db logical replication logs.

        Here, we are grouping all rows of the same table and tg_op
        and processing them as a group in bulk.
        This is more efficient.
        e.g [
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': DELETE, 'table': A, ...},
            {'tg_op': DELETE, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
        ]

        We will have 3 groups being synced together in one execution
        First 3 INSERT, Next 2 DELETE and then the next 2 INSERT.
        Perhaps this could be improved but this is the best approach so far.

        TODO: We can also process all INSERTS together and rearrange
        them as done below
        """
        rows = self.logical_slot_peek_changes(
            self.__name,
            upto_nchanges=QUERY_CHUNK_SIZE,
        )

        rows = rows or []
        payloads = []
        _rows = []
        _parsed = []

        for row in rows:
            if (
                re.search(r'^BEGIN', row.data) or
                re.search(r'^COMMIT', row.data)
            ):
                continue
            _rows.append(row)

            try:
                _parsed.append(self.parse_logical_slot(row.data))
            except Exception as e:
                logger.exception(
                    f'Error parsing row: {e}\nRow data: {row.data}'
                )
                raise

        # Sort all rows by their table (since tables are independent of each other)
        parsed_rows = sorted(zip(_rows, _parsed), key=lambda rp: rp[1]["table"])

        to_delete = set()
        filters = collections.defaultdict(set)

        # Accumulate the list of root PKs to delete and tables to invalidate.
        for row, payload in parsed_rows:
            # logger.debug(f'txid: {row.xid}')
            # logger.debug(f'data: {row.data}')

            if payloads and (
                payloads[-1]["tg_op"] != payload["tg_op"]
                or payloads[-1]["table"] != payload["table"]
            ):
                to_delete, filters = self._get_operations(payloads, to_delete, filters)
                payloads = []

            payloads.append(payload)

        if payloads:
            to_delete, filters = self._get_operations(payloads, to_delete, filters)

        filters = {k: [dict(v) for v in vs] for k, vs in filters.items()}

        logger.debug("to_delete: %s, filters: %s", to_delete, filters)

        # Delete rows that are gone
        if to_delete:
            docs = [self._make_delete_doc(d) for d in to_delete]
            logging.info("Deleting %d documents")
            self.es.bulk(self.index, docs)

        if any(filters.values()):
            if any(len(v) > 1000 for v in filters.values()):
                # No point in sending a query with 1000 ORs to PG, just reload the whole thing.
                logging.warning(
                    "Some of the entries in the filters list are too long. Doing a full resync."
                )
                filters = None
            docs = self._sync(
                self.nodes,
                self.index,
                filters=filters,
            )
            self.es.bulk(self.index, docs)

        if rows:
            self.logical_slot_get_changes(
                self.__name,
                upto_nchanges=len(rows),
            )
        return len(rows)

    def _get_operations(self, payloads, to_delete=None, filters=None):
        to_delete = to_delete or set()
        filters = filters or collections.defaultdict(set)
        this_to_delete, this_filters = self._payloads(self.nodes, self.index, payloads)

        to_delete.update(this_to_delete)

        for table, table_filters in this_filters.items():
            filters[table].update({tuple(v.items()) for v in table_filters})

        return to_delete, filters

    def _payload_data(self, payload):
        """Extract the payload data from the payload."""
        payload_data = payload.get('new')
        if payload['tg_op'] == DELETE:
            if payload.get('old'):
                payload_data = payload.get('old')
        return payload_data

    def _payloads(self, nodes, index, payloads):
        """
        The "payloads" is a list of payload operations to process together.

        The basic assumption is that all payloads in the list have the
        same tg_op and table name.

        e.g:
        [
            {
                'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 1}'new': {'id': 4}
            },
            {
                'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 2}'new': {'id': 5}
            },
            {   'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 3}'new': {'id': 6},
            }
            ...
        ]

        """
        payload = payloads[0]
        tg_op = payload['tg_op']
        if tg_op not in TG_OP:
            logger.exception(f'Unknown tg_op {tg_op}')
            raise

        table = payload['table']
        schema = payload['schema']

        model = self.model(table, schema)

        root_table = nodes[0]['table']
        root_model = self.model(
            root_table,
            nodes[0].get('schema', SCHEMA),
        )

        for payload in payloads:
            payload_data = self._payload_data(payload)
            # this is only required for the non truncate tg_ops
            if payload_data:
                if not set(model.primary_keys).issubset(
                    set(payload_data.keys())
                ):
                    table = payload['table']
                    schema = payload['schema']
                    logger.exception(
                        f'Primary keys {model.primary_keys} not subset of '
                        f'payload data {payload_data.keys()} for table '
                        f'{schema}.{table}'
                    )
                    raise

        logger.debug(f'tg_op: {tg_op} table: {schema}.{table}')

        # we might receive an event triggered for a table
        # that is not in the tree node.
        # e.g a through table which we need to react to.
        # in this case, we find the parent of the through
        # table and force a re-sync.
        filters = {table: [], root_table: []}
        to_delete = []

        if (
            table not in self.tree.nodes and
            table not in self.tree.through_nodes
        ):
            logger.debug("table not in nodes: %s %s", self.tree.nodes, self.tree.through_nodes)
            return to_delete, filters

        if tg_op == INSERT:
            if table == root_table:
                for payload in payloads:
                    self._invalidate_self(filters, model, payload, table)
            else:
                node = self._find_parent_node(nodes, table)
                self._invalidate_parent(schema, table, node, payloads, filters)

        if tg_op == UPDATE:

            if table == root_table:
                # Here, we are performing two operations:
                # 1) Build a filter to sync the updated record(s)
                # 2) Delete the old record(s) in Elasticsearch if the
                #    primary key has changed
                #   2.1) This is crucial otherwise we can have the old
                #        and new document in Elasticsearch at the same time
                for payload in payloads:
                    self._invalidate_self(filters, model, payload, table)

                    old_values = []
                    for key in root_model.primary_keys:
                        if key in payload["old"].keys():
                            old_values.append(payload["old"][key])

                    new_values = [
                        payload["new"][key] for key in root_model.primary_keys
                    ]

                    if (
                        len(old_values) == len(new_values) and
                        old_values != new_values
                    ):

                        to_delete.append(tuple(old_values))

            else:
                # NB this breaks if there's been a PK change
                node = self._find_parent_node(nodes, table)
                self._invalidate_parent(schema, table, node, payloads, filters)

        if tg_op == DELETE:
            # when deleting a root node, just delete the doc in Elasticsearch
            if table == root_table:
                for payload in payloads:

                    payload_data = self._payload_data(payload)
                    root_primary_values = [
                        payload_data[key] for key in root_model.primary_keys
                    ]
                    to_delete.append(tuple(root_primary_values))
            else:
                # NB this breaks if the child has a PK constraint not on on its FKs
                # (since then we've no idea what the old values were and what to invalidate)
                node = self._find_parent_node(nodes, table)
                self._invalidate_parent(schema, table, node, payloads, filters)

        if tg_op == TRUNCATE:
            if table == root_table:
                for doc_id in self.es._search(index, table, {}):

                    doc = {
                        '_id': doc_id,
                        '_index': index,
                        '_op_type': 'delete',
                    }
                    if self.es.version[0] < 7:
                        doc['_type'] = '_doc'
                    to_delete.append(tuple(doc_id.split(PRIMARY_KEY_DELIMITER)))

            else:
                _filters = []
                for doc_id in self.es._search(index, table, {}):
                    where = {}
                    params = doc_id.split(PRIMARY_KEY_DELIMITER)
                    for i, key in enumerate(root_model.primary_keys):
                        where[key] = params[i]

                    _filters.append(where)

                if _filters:
                    filters[root_table].extend(_filters)

        return to_delete, filters

    def _invalidate_self(self, filters, model, payload, table):
        payload_data = self._payload_data(payload)
        primary_values = [
            payload_data[key] for key in model.primary_keys
        ]
        primary_fields = dict(
            zip(model.primary_keys, primary_values)
        )
        filters[table].append({
            key: value for key, value in primary_fields.items()
        })

    def _invalidate_parent(self, schema, table, node, payloads, filters):
        filters[node.parent.table] = []
        foreign_keys = self.query_builder._get_foreign_keys(
            node.parent,
            node,
        )
        _table = self._absolute_table(schema, table)
        node_parent_table = self._absolute_table(
            node.parent.schema,
            node.parent.table,
        )
        for payload in payloads:
            payload_data = self._payload_data(payload)
            filters[node.parent.table].append({
                foreign_keys[node_parent_table][i]: payload_data[key] for
                i, key in enumerate(foreign_keys[_table])
            })

    def _find_parent_node(self, nodes, table):
        root = self.tree.build(nodes[0])
        for node in traverse_post_order(root):
            if table == node.table or table in node.relationship.through_tables:
                if not node.parent:
                    logger.exception(
                        f'Could not get parent from node: {node.name}'
                    )
                    raise
                return node

    def _build_filters(self, filters, node):
        """
        Build SQLAlchemy filters.

        NB:
        assumption dictionary is an AND and list is an OR

        filters['book'] = [
            {'id': 1, 'uid': '001'},
            {'id': 2, 'uid': '002'}
        ]
        """
        _filters = []
        if filters.get(node.table):

            for _filter in filters.get(node.table):
                where = []
                for key, value in _filter.items():
                    where.append(
                        getattr(node.model.c, key) == value
                    )
                _filters.append(
                    sa.and_(*where)
                )

            node._filters.append(
                sa.or_(*_filters)
            )

    def _sync(
        self,
        nodes,
        index,
        filters=None,
        txmin=None,
        txmax=None,
        extra=None,
    ):
        if filters is None:
            filters = {}

        root = self.tree.build(nodes[0])

        self.query_builder.isouter = True

        for node in traverse_post_order(root):

            self._build_filters(filters, node)

            if node.is_root:
                if txmin:
                    node._filters.append(
                        sa.cast(
                            sa.cast(
                                node.model.c.xmin,
                                sa.Text,
                            ), sa.BigInteger,
                        ) >= txmin
                    )
                if txmax:
                    node._filters.append(
                        sa.cast(
                            sa.cast(
                                node.model.c.xmin,
                                sa.Text,
                            ), sa.BigInteger,
                        ) < txmax
                    )

            try:
                self.query_builder.build_queries(node)
            except Exception as e:
                logger.exception(f'Exception {e}')
                raise

        if self.verbose:
            compiled_query(node._subquery, 'Query')

        row_count = self.query_count(node._subquery)

        for i, (keys, row, primary_keys) in enumerate(
            self.query_yield(node._subquery)
        ):

            if i % 1000 == 0:
                progress(i + 1, row_count)

            row = transform(row, nodes[0])
            row[META] = get_private_keys(keys)
            if extra:
                if extra['table'] not in row[META]:
                    row[META][extra['table']] = {}
                if extra['column'] not in row[META][extra['table']]:
                    row[META][extra['table']][extra['column']] = []
                row[META][extra['table']][extra['column']].append(0)

            if self.verbose:
                print(f'{(i+1)})')
                print(f'Pkeys: {primary_keys}')
                pprint.pprint(row)
                print('-' * 10)

            doc = {
                '_id': self.get_doc_id(primary_keys),
                '_index': index,
                '_source': row,
            }

            if self.es.version[0] < 7:
                doc['_type'] = '_doc'

            if self._plugins:
                doc = list(self._plugins.transform([doc]))[0]

            if self.pipeline:
                doc['pipeline'] = self.pipeline

            yield doc

    def full_sync(self):
        """
        Pull sync all data from database.

        main entry point.
        sync all tables as docs to Elasticsearch
        document contains -> nodes:
        nodes contains -> node
        """

        # We want to reload all documents into the index + delete documents that we didn't
        # reload. Since we don't want to delete the index or empty it out (clients will see
        # an empty index for a few seconds), we instead load data with a timestamp and then
        # delete all docs that don't have that timestamp.

        timestamp = datetime.now().isoformat()
        logger.info("Resyncing index %s, timestamp %s", self.index, timestamp)

        docs = self._sync(
            self.nodes,
            self.index,
            timestamp=timestamp
        )
        self.es.bulk(self.index, docs)

        # Now bulk delete docs without the new timestamp
        logger.info("Deleting old documents in index %s (timestamp not %s)",
                    self.index, timestamp)
        self.es.delete_old(self.index, timestamp)
        logger.info("Full resync for index %s complete", self.index)

    def _make_delete_doc(self, pk):
        doc = {
            '_id': self.get_doc_id(pk),
            '_index': self.index,
            '_op_type': 'delete',
        }
        if self.es.version[0] < 7:
            doc['_type'] = '_doc'
        return doc

    @property
    def checkpoint(self):
        """Save the current txid as the checkpoint."""
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, 'r') as fp:
                self._checkpoint = int(fp.read().split()[0])
        return self._checkpoint

    @checkpoint.setter
    def checkpoint(self, value=None):
        if value is None:
            raise ValueError('Cannot assign a None value to checkpoint')
        with open(self._checkpoint_file, 'w+') as fp:
            fp.write(f'{value}\n')
        self._checkpoint = value

    @threaded
    def poll_db(self):
        """
        Producer which polls the replication slot and forwards messages to ElasticSearch
        """
        txmin = self.checkpoint
        txmax = self.txid_current
        total_rows = 0
        last_message = datetime.now()

        while True:
            time.sleep(POLL_TIMEOUT)
            logger.debug("Syncing slot %s from %s to %s", self.__name, txmin, txmax)
            #
            # # forward pass sync
            # self.sync(txmin=txmin, txmax=txmax)
            # # Sync up to txmax
            while True:
                rows = self.logical_slot_changes()
                logger.debug("Synced %s row(s) from slot %s", rows, self.__name)
                total_rows += rows
                if not rows:
                    break

            # self.checkpoint = txmax
            # txmin = txmax
            # txmax = self.txid_current

            now = datetime.now()
            if (now - last_message).total_seconds() > 60:
                logging.info("Last 60s: synced %s row(s) from slot %s, txmin %s, txmax %s",
                             total_rows, self.__name, txmin, txmax)
                total_rows = 0
                last_message = now

    def pull(self):
        """Pull data from db."""
        txmin = self.checkpoint
        txmax = self.txid_current
        logger.info("Initial sync, txmin = %s, txmax=%s", txmin, txmax)
        # forward pass sync
        self.sync(txmin=txmin, txmax=txmax)
        # now sync up to txmax to capture everything we might have missed
        self.logical_slot_changes()
        self._truncate = True

    @threaded
    def truncate_slots(self):
        """Truncate the logical replication slot."""
        while True:
            if self._truncate and (
                datetime.now() >= self._last_truncate_timestamp + timedelta(
                    seconds=REPLICATION_SLOT_CLEANUP_INTERVAL
                )
            ):
                logger.debug(f'Truncating replication slot: {self.__name}')
                self.logical_slot_get_changes(self.__name, upto_nchanges=None)
                self._last_truncate_timestamp = datetime.now()
            time.sleep(0.1)

    def receive(self):
        """
        Receive events from db.
        """

        # If the replication slot doesn't exist, we create it, then do a full reload
        if not self.replication_slots(self.__name):
            logging.info(
                "Replication slot %s doesn't exist. Recreating and resyncing.",
                self.__name
            )
            self.create_replication_slot(self.__name)
            self.full_sync()
        else:
            logging.info("Replication slot %s exists.", self.__name)

        return self.poll_db()


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
@click.option('--daemon', '-d', is_flag=True, help='Run as a daemon')
@click.option('--host', '-h', help='PG_HOST override')
@click.option('--password', is_flag=True, help='Prompt for database password')
@click.option('--port', '-p', help='PG_PORT override', type=int)
@click.option(
    '--sslmode',
    help='PG_SSLMODE override',
    type=click.Choice(
        [
            'allow',
            'disable',
            'prefer',
            'require',
            'verify-ca',
            'verify-full',
        ],
        case_sensitive=False,
    ),
)
@click.option(
    '--sslrootcert',
    help='PG_SSLROOTCERT override',
    type=click.Path(exists=True),
)
@click.option('--user', '-u', help='PG_USER override')
@click.option(
    '--verbose',
    '-v',
    is_flag=True,
    default=False,
    help='Turn on verbosity',
)
@click.option(
    '--version',
    is_flag=True,
    default=False,
    help='Show version info',
)
def main(
    config,
    daemon,
    host,
    password,
    port,
    sslmode,
    sslrootcert,
    user,
    verbose,
    version,
):
    """
    main application syncer
    """
    if version:
        sys.stdout.write(f'Version: {__version__}\n')
        return

    params = {
        'user': user,
        'host': host,
        'port': port,
        'sslmode': sslmode,
        'sslrootcert': sslrootcert,
    }
    if password:
        params['password'] = click.prompt(
            "Password",
            type=str,
            hide_input=True,
        )
    params = {
        key: value for key, value in params.items() if value is not None
    }

    config = get_config(config)

    show_settings(config, params)

    with Timer():
        for document in json.load(open(config)):
            sync = Sync(
                document,
                verbose=verbose,
                params=params,
            )
            sync.pull()
            if daemon:
                sync.receive()


if __name__ == '__main__':
    main()
