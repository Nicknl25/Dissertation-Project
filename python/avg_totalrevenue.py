import os
import sys
from typing import List, Optional, Tuple

import platform

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def _env(key: str) -> Optional[str]:
    v = os.getenv(key)
    if v is None:
        return None
    v = v.strip()
    return v or None


def load_env() -> None:
    if load_dotenv:
        try:
            load_dotenv()
        except Exception as ex:
            eprint(f"Warning: could not load .env: {ex}")


def connect_mysql():
    import mysql.connector  # lazy import to fail fast with clear message

    host = _env("MYSQL_HOST")
    user = _env("MYSQL_USER")
    password = _env("MYSQL_PASSWORD")
    database = _env("MYSQL_DB")

    missing = [k for k, v in (
        ("MYSQL_HOST", host),
        ("MYSQL_USER", user),
        ("MYSQL_PASSWORD", password),
        ("MYSQL_DB", database),
    ) if not v]
    if missing:
        eprint("Missing required environment variables:", ", ".join(missing))
        eprint("Ensure they are present in .env or your environment.")
        sys.exit(2)

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=True,
        )
        return conn
    except Exception as ex:  # mysql.connector.Error
        eprint("Failed to connect to MySQL.")
        eprint(f"Host={host} DB={database} User={user}")
        eprint(f"Python={platform.python_version()} Platform={platform.platform()}")
        eprint(f"Error: {type(ex).__name__}: {ex}")
        sys.exit(1)


def find_tables_with_totalrevenue(conn, database: str) -> List[str]:
    sql = (
        "SELECT table_name "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND LOWER(column_name) = 'totalrevenue' "
        "ORDER BY table_name"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (database,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def compute_avg_for_table(conn, table: str) -> Tuple[Optional[float], Optional[str]]:
    # Quote table name safely (defensive; names are from information_schema)
    safe_table = "`" + table.replace("`", "``") + "`"
    queries = [
        f"SELECT AVG(CAST(totalrevenue AS DECIMAL(32,8))) FROM {safe_table}",
        f"SELECT AVG(totalrevenue) FROM {safe_table}",
    ]
    with conn.cursor() as cur:
        for q in queries:
            try:
                cur.execute(q)
                (val,) = cur.fetchone()
                if val is None:
                    return None, None
                try:
                    # Ensure it's a Python float
                    return float(val), None
                except Exception:
                    return None, "Non-numeric AVG result"
            except Exception as ex:
                last_err = str(ex)
        return None, last_err  # type: ignore[name-defined]


def main() -> int:
    load_env()

    # Gather credentials (and for info only)
    host = _env("MYSQL_HOST") or "?"
    db = _env("MYSQL_DB") or "?"

    # Connect
    conn = connect_mysql()
    try:
        tables = find_tables_with_totalrevenue(conn, _env("MYSQL_DB") or "")
        if not tables:
            print("No table with a 'totalrevenue' column was found.")
            return 0

        # If multiple, compute for each
        printed_any = False
        for t in tables:
            avg, err = compute_avg_for_table(conn, t)
            if err:
                eprint(f"Skipping {t}: {err}")
                continue
            if avg is None:
                print(f"Average Total Revenue ({t}): $0.00")
            else:
                print(f"Average Total Revenue ({t}): ${avg:,.2f}")
            printed_any = True

        if not printed_any:
            print("Could not compute an average due to data/type issues.")
            return 1

        return 0

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

