import os
import sys
from typing import Optional

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


def getenv(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    return v or None


def main() -> int:
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass

    host = getenv("MYSQL_HOST")
    user = getenv("MYSQL_USER")
    password = getenv("MYSQL_PASSWORD")
    database = getenv("MYSQL_DB")

    missing = [k for k, v in (
        ("MYSQL_HOST", host),
        ("MYSQL_USER", user),
        ("MYSQL_PASSWORD", password),
        ("MYSQL_DB", database),
    ) if not v]
    if missing:
        print(f"Missing required variables: {', '.join(missing)}", file=sys.stderr)
        return 2

    import mysql.connector  # type: ignore

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
        )
    except Exception as ex:
        print(f"Failed to connect to MySQL: {ex}", file=sys.stderr)
        return 1

    try:
        cur = conn.cursor()

        # Check if index already exists
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
              AND index_name = %s
            """,
            (database, "Dissertation_Data", "idx_symbol_fiscalDate"),
        )
        (count_existing,) = cur.fetchone()
        if count_existing and int(count_existing) > 0:
            print("Index already exists: idx_symbol_fiscalDate")
            return 0

        # Create the unique index
        cur.execute(
            "ALTER TABLE `Dissertation_Data`\n"
            "ADD UNIQUE INDEX `idx_symbol_fiscalDate` (`symbol`, `fiscalDateEnding`)"
        )
        conn.commit()

        # Verify creation
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
              AND index_name = %s
            """,
            (database, "Dissertation_Data", "idx_symbol_fiscalDate"),
        )
        (count_after,) = cur.fetchone()
        if count_after and int(count_after) > 0:
            print("Index created successfully: idx_symbol_fiscalDate")
            return 0
        else:
            print("Index creation attempted but not verified.", file=sys.stderr)
            return 1

    except Exception as ex:
        # Helpful message if duplicates exist
        msg = str(ex)
        print(f"Error creating index: {msg}", file=sys.stderr)
        try:
            cur2 = conn.cursor()
            cur2.execute(
                """
                SELECT symbol, fiscalDateEnding, COUNT(*) AS c
                FROM `Dissertation_Data`
                GROUP BY symbol, fiscalDateEnding
                HAVING c > 1
                LIMIT 10
                """
            )
            dups = cur2.fetchall()
            if dups:
                print("Sample duplicate keys (up to 10):", file=sys.stderr)
                for row in dups:
                    print(f"  symbol={row[0]} fiscalDateEnding={row[1]} count={row[2]}", file=sys.stderr)
        except Exception:
            pass
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

