#!/usr/bin/env python3
"""
AutoDBA v5 — Ultimate Self-Healing Database Index Optimizer
===========================================================
Tails proxy_engine/build/query_logs.json, parses queries, and injects optimal indexes.
Fixes the "Filesort" and nested loop bottlenecks by intelligently classifying columns
(WHERE, JOIN, ORDER BY) and creating precise DB-Admin level composite index variations:
  1. Drives:  (WHERE + ORDER BY)
  2. Driven:  (JOIN + WHERE)
"""

import os
import re
import sys
import json
import time
import mysql.connector
from mysql.connector import Error

# ── Windows ANSI Support ─────────────────────────────────────────────────────
os.system('')  # Enables virtual-terminal processing on Windows 10+

# ── ANSI Color Codes ─────────────────────────────────────────────────────────
GREEN  = '\033[92m'
YELLOW = '\033[93m'
RED    = '\033[91m'
CYAN   = '\033[96m'
BOLD   = '\033[1m'
DIM    = '\033[2m'
RESET  = '\033[0m'

# ── Configuration ────────────────────────────────────────────────────────────
LOG_FILE = 'proxy_engine/build/query_logs.json'

DB_CONFIG = {
    'host':     'localhost',
    'user':     'autodba_admin',
    'password': 'StrongPassword123!',
    'database': 'autodba_test',
    'port':     3306,
}

SLOW_QUERY_THRESHOLD_MS = 500.0

_SQL_KEYWORDS = frozenset({
    'ON', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
    'SET', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'ORDER', 'GROUP',
    'HAVING', 'LIMIT', 'UNION', 'AS', 'SELECT', 'FROM', 'INTO', 'VALUES',
    'UPDATE', 'DELETE', 'INSERT', 'CREATE', 'DROP', 'ALTER', 'INDEX',
    'TABLE', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IS',
    'NULL', 'TRUE', 'FALSE', 'ASC', 'DESC', 'DISTINCT', 'ALL', 'ANY',
    'USING', 'NATURAL', 'FULL', 'WITH', 'RECURSIVE', 'FETCH', 'OFFSET',
    'FOR', 'LOCK', 'SHARE', 'KEY', 'PRIMARY', 'FOREIGN', 'REFERENCES',
    'CASCADE', 'RESTRICT', 'NO', 'ACTION', 'DEFAULT', 'CHECK', 'UNIQUE',
    'CONSTRAINT', 'BY', 'TO', 'IF', 'REPLACE', 'IGNORE', 'TEMPORARY',
})

class IndexCache:
    def __init__(self):
        self._known_single: set[str] = set()
        self._known_composite: set[str] = set()
        self._stats = {'hits': 0, 'misses': 0}

    @staticmethod
    def _single_key(table: str, column: str) -> str:
        return f"{table.lower()}.{column.lower()}"

    @staticmethod
    def _composite_key(table: str, columns: list[str]) -> str:
        return f"{table.lower()}.{'_'.join(c.lower() for c in columns)}"

    def contains_single(self, table: str, column: str) -> bool:
        key = self._single_key(table, column)
        if key in self._known_single:
            self._stats['hits'] += 1
            return True
        self._stats['misses'] += 1
        return False

    def contains_composite(self, table: str, columns: list[str]) -> bool:
        key = self._composite_key(table, columns)
        if key in self._known_composite:
            self._stats['hits'] += 1
            return True
        self._stats['misses'] += 1
        return False

    def add_single(self, table: str, column: str) -> None:
        self._known_single.add(self._single_key(table, column))

    def add_composite(self, table: str, columns: list[str]) -> None:
        self._known_composite.add(self._composite_key(table, columns))

    def preload(self, conn) -> int:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, INDEX_NAME, SEQ_IN_INDEX
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
        """, (DB_CONFIG['database'],))
        rows = cursor.fetchall()
        cursor.close()

        for row in rows:
            self.add_single(row['TABLE_NAME'], row['COLUMN_NAME'])

        composite_map: dict[str, list[str]] = {}
        for row in rows:
            key = f"{row['TABLE_NAME']}.{row['INDEX_NAME']}"
            if key not in composite_map:
                composite_map[key] = []
            composite_map[key].append(row['COLUMN_NAME'])

        for key, cols in composite_map.items():
            table = key.split('.')[0]
            if len(cols) > 1:
                self.add_composite(table, cols)
        return len(rows)

    @property
    def size(self) -> int:
        return len(self._known_single) + len(self._known_composite)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

def clean_query(raw: str) -> str:
    return re.sub(r'^[^\x20-\x7E]+', '', raw).strip()

def parse_alias_map(sql: str) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    pattern = re.compile(
        r'(?:FROM|JOIN)\s+(\w+)(?:\s+AS\s+(\w+))?(?:\s+(\w+))?',
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        table = m.group(1).upper()
        alias_as = m.group(2)
        alias_bare = m.group(3)
        alias = (alias_as or alias_bare or '').upper()
        if alias and alias not in _SQL_KEYWORDS:
            alias_map[alias] = table
        else:
            alias_map[table] = table
    return alias_map

def resolve_alias(alias: str, alias_map: dict[str, str]) -> str | None:
    return alias_map.get(alias.upper())

def extract_all_targets(sql: str) -> dict[str, dict[str, list[str]]]:
    """
    Groups columns by usage perfectly:
    returns { 'TABLE': {'where': [cols], 'join': [cols], 'order': [cols]} }
    """
    alias_map = parse_alias_map(sql)
    table_columns: dict[str, dict[str, list[str]]] = {}
    
    def _add(table: str, category: str, column: str) -> None:
        if table not in table_columns:
            table_columns[table] = {'where': [], 'join': [], 'order': []}
        if column not in table_columns[table][category]:
            table_columns[table][category].append(column)

    # 1. WHERE / AND / OR 
    cond_qual = re.compile(
        r'(?:WHERE|AND|OR)\s+(\w+)\.(\w+)\s*(?:=|!=|<>|<=|>=|<|>|LIKE|IN|BETWEEN|IS)',
        re.IGNORECASE,
    )
    for m in cond_qual.finditer(sql):
        table = resolve_alias(m.group(1).upper(), alias_map)
        if table: _add(table, 'where', m.group(2).upper())

    # 2. JOIN ON 
    on_re = re.compile(
        r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        re.IGNORECASE,
    )
    for m in on_re.finditer(sql):
        t1 = resolve_alias(m.group(1).upper(), alias_map)
        t2 = resolve_alias(m.group(3).upper(), alias_map)
        if t1: _add(t1, 'join', m.group(2).upper())
        if t2: _add(t2, 'join', m.group(4).upper())

    # 3. ORDER BY 
    orderby_re = re.compile(
        r'ORDER\s+BY\s+(.*?)(?:LIMIT|$)',
        re.IGNORECASE | re.DOTALL,
    )
    orderby_match = orderby_re.search(sql)
    if orderby_match:
        for m in re.finditer(r'(\w+)\.(\w+)', orderby_match.group(1)):
            table = resolve_alias(m.group(1).upper(), alias_map)
            if table: _add(table, 'order', m.group(2).upper())

    return table_columns

class DBConnection:
    def __init__(self):
        self._conn = None

    def get(self):
        if self._conn is None or not self._conn.is_connected():
            try:
                self._conn = mysql.connector.connect(**DB_CONFIG)
                print(f"{GREEN}[+] Database connection established.{RESET}")
            except Error as e:
                print(f"{RED}[-] MySQL connection failed: {e}{RESET}")
                self._conn = None
        return self._conn

    def close(self):
        if self._conn and self._conn.is_connected():
            self._conn.close()
            print(f"{DIM}[*] Database connection closed.{RESET}")

def inject_single_index(db: DBConnection, cache: IndexCache, table: str, column: str) -> bool:
    if cache.contains_single(table, column): return False
    conn = db.get()
    if conn is None: return False
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT COUNT(1) AS idx_exists FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """, (DB_CONFIG['database'], table, column))
        if cursor.fetchone()['idx_exists'] > 0:
            cache.add_single(table, column)
            return False
            
        index_name = f"auto_idx_{table}_{column}".lower()
        print(f"{YELLOW}    → Creating single index: {index_name}{RESET}")
        cursor.execute(f"CREATE INDEX `{index_name}` ON `{table}`(`{column}`)")
        conn.commit()
        cache.add_single(table, column)
        return True
    except Error as e:
        if e.errno == 1061: cache.add_single(table, column)
        return False
    finally:
        if 'cursor' in locals(): cursor.close()

def inject_composite_index(db: DBConnection, cache: IndexCache, table: str, columns: list[str]) -> bool:
    if len(columns) < 2 or cache.contains_composite(table, columns): return False
    conn = db.get()
    if conn is None: return False
    try:
        cursor = conn.cursor(dictionary=True)
        col_suffix = '_'.join(c.lower() for c in columns)
        index_name = f"auto_cidx_{table}_{col_suffix}".lower()[:64]
        
        cursor.execute("""
            SELECT COUNT(1) AS idx_exists FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND INDEX_NAME = %s
        """, (DB_CONFIG['database'], index_name))
        if cursor.fetchone()['idx_exists'] > 0:
            cache.add_composite(table, columns)
            return False

        col_list = ', '.join(f'`{c}`' for c in columns)
        print(f"{YELLOW}{BOLD}    → Creating DB-Admin COMPOSITE index: {index_name}{RESET}")
        print(f"{YELLOW}      Pattern (Equality + Range): ({col_list}) on `{table}`{RESET}")
        cursor.execute(f"CREATE INDEX `{index_name}` ON `{table}`({col_list})")
        conn.commit()
        cache.add_composite(table, columns)
        print(f"{GREEN}{BOLD}    ✓ COMPOSITE index {index_name} deployed!{RESET}")
        return True
    except Error as e:
        if e.errno == 1061: cache.add_composite(table, columns)
        return False
    finally:
        if 'cursor' in locals(): cursor.close()

def process_slow_query(db: DBConnection, cache: IndexCache, query: str, duration: float) -> int:
    cleaned = clean_query(query)
    targets = extract_all_targets(cleaned)
    if not targets: return 0

    created = 0
    for table, categorized in targets.items():
        all_cols = list(set(categorized['where'] + categorized['join'] + categorized['order']))
        
        # 1. Single indexes
        for col in all_cols:
            if inject_single_index(db, cache, table, col):
                created += 1

        # 2. Perfect Composite Patterns
        where_cols = sorted(categorized['where'])
        join_cols = sorted(categorized['join'])
        order_cols = sorted(categorized['order'])

        # Pattern A (Driving Table): WHERE + ORDER BY (Provides filesort avoidance)
        if where_cols and order_cols:
            if inject_composite_index(db, cache, table, where_cols + order_cols):
                created += 1

        # Pattern B (Driven Table Lookup): JOIN + WHERE (Provides lightning nested loop)
        if join_cols and where_cols:
            for jcol in join_cols:
                if inject_composite_index(db, cache, table, [jcol] + where_cols):
                    created += 1

        # Pattern C (Ultra Covering): WHERE + JOIN + ORDER BY
        if len(all_cols) >= 2:
            # Try a combined index to cover all operations for this table!
            combo = where_cols + join_cols + order_cols
            # deduplicate while preserving order (equality -> lookup -> sort)
            ordered_combo = list(dict.fromkeys(combo))
            if len(ordered_combo) > len(where_cols + order_cols): # Only if it adds value over Pattern A
                if inject_composite_index(db, cache, table, ordered_combo):
                    created += 1
    return created

def tail_log_file(db: DBConnection, cache: IndexCache) -> None:
    print(f"{CYAN}[*] Waiting for log file: {LOG_FILE}{RESET}")
    while not os.path.exists(LOG_FILE):
        time.sleep(1)
    print(f"{GREEN}[+] Log file detected. Tailing...{RESET}\n")

    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.05)
                continue
            line = line.strip()
            if not line: continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError: continue

            duration = entry.get('duration_ms', 0)
            if duration < SLOW_QUERY_THRESHOLD_MS: continue

            query = entry.get('query', '')
            cleaned = clean_query(query)
            targets = extract_all_targets(cleaned)
            
            # Smart Cache Verification
            has_uncached = False
            for table, categorized in targets.items():
                where = sorted(categorized['where'])
                join = sorted(categorized['join'])
                order = sorted(categorized['order'])
                all_cols = list(set(where + join + order))

                for col in all_cols:
                    if not cache.contains_single(table, col):
                        has_uncached = True; break

                # Check if crucial composites are missing
                if where and order and not cache.contains_composite(table, where + order):
                    has_uncached = True; break
                if join and where:
                    for jcol in join:
                        if not cache.contains_composite(table, [jcol] + where):
                            has_uncached = True; break
                            
                # Check if Pattern C (Ultra Covering) is missing
                combo = where + join + order
                ordered_combo = list(dict.fromkeys(combo))
                if len(ordered_combo) > 2 and len(ordered_combo) > len(where + order):
                    if not cache.contains_composite(table, ordered_combo):
                        has_uncached = True; break
                
                if has_uncached: break

            if has_uncached:
                print(f"\n{RED}{BOLD}[SLOW] {duration:.1f} ms{RESET}")
                print(f"{DIM}       {cleaned[:120]}...{RESET}")
                
                created = process_slow_query(db, cache, query, duration)
                if created > 0:
                    print(f"{GREEN}{BOLD}[★] {created} new smart index(es) injected. Database self-healed!{RESET}")
            else:
                # --- THE SENIOR DBA KICKER ---
                # Indexes exist but MySQL is ignoring them! Force it to update stats.
                for table in targets.keys():
                    kick_key = f"analyzed_{table}"
                    if kick_key not in cache._stats:
                        cache._stats[kick_key] = True
                        print(f"{YELLOW}[!] MySQL ignoring indexes! Forcing ANALYZE TABLE `{table}`...{RESET}")
                        try:
                            c = db.get().cursor()
                            c.execute(f"ANALYZE TABLE `{table}`")
                            c.fetchall()  # <--- THIS IS THE MAGIC FIX. WE MUST READ THE RECEIPT!
                            c.close()
                            print(f"{GREEN}[+] Optimizer stats refreshed for {table}.{RESET}")
                        except Exception as e: 
                            print(f"{RED}[-] Failed to analyze {table}: {e}{RESET}")
                # ----------------------------------

def main() -> None:
    print(f"{CYAN}{BOLD}╔══════════════════════════════════════════════════════════════════╗")
    print(f"║          AutoDBA v5 — Ultimate Index Optimizer                  ║")
    print(f"║   Smart Permutations • Filesort Destroyer • Instant ANALYZE     ║")
    print(f"╚══════════════════════════════════════════════════════════════════╝{RESET}\n")

    db = DBConnection()
    cache = IndexCache()
    if db.get():
        count = cache.preload(db.get())
        print(f"{GREEN}[+] Pre-loaded {count} existing index entries.{RESET}")

    print(f"{CYAN}[*] Threshold: {SLOW_QUERY_THRESHOLD_MS} ms{RESET}")
    print(f"{CYAN}[*] Strategy: AI-Targeted Optimal Prefix Composites{RESET}")
    print(f"{DIM}    Press Ctrl+C to shut down.{RESET}\n")

    try:
        tail_log_file(db, cache)
    except KeyboardInterrupt:
        print(f"\n{CYAN}{BOLD}[*] AutoDBA Shutting Down.{RESET}")
        db.close()

if __name__ == '__main__':
    main()