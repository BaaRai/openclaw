import { execFileSync } from "node:child_process";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { afterEach, expect, it } from "vitest";
import { trackSqliteStatementExecutions } from "../../../test/helpers/sqlite-statement-execution-counter.js";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import { openNodeSqliteDatabase } from "../../infra/node-sqlite.js";
import { withSqlitePostCommitPublications } from "../../infra/sqlite-post-commit.js";
import { admitSqliteSchema, runSqliteReadOperationSync } from "../../infra/sqlite-schema-facts.js";
import { runSqliteImmediateTransactionSync } from "../../infra/sqlite-transaction.js";
import { createSessionEntryRevisionGuard } from "./session-accessor.sqlite-entry-revision.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
const databases: DatabaseSync[] = [];
afterEach(() => {
  for (const database of databases.splice(0)) {
    database.close();
  }
});

function fixture(filename = ":memory:") {
  const database = openNodeSqliteDatabase(filename);
  databases.push(database);
  database.exec("CREATE TABLE session_nodes (id INTEGER PRIMARY KEY, writer TEXT)");
  database.exec("INSERT INTO session_nodes VALUES (1, 'current')");
  admitSqliteSchema(database);
  let current = true;
  const guard = createSessionEntryRevisionGuard(
    database,
    () => {
      if (!current) {
        throw new Error("source released");
      }
    },
    () =>
      database.prepare("SELECT writer FROM session_nodes WHERE id = 1").get()?.writer === "current",
  );
  guard();
  return {
    database,
    guard,
    release: () => {
      current = false;
    },
    transaction: <T>(operation: () => T) =>
      withSqlitePostCommitPublications(database, () =>
        runSqliteImmediateTransactionSync(database, operation),
      ),
  };
}

it("shares transaction freshness while detecting local writes, rollback, and released authority", () => {
  const { database, guard, transaction, release } = fixture();
  const sql = trackSqliteStatementExecutions(database, ["fresh"], (statement) =>
    /^PRAGMA data_version$/iu.test(statement.trim()) ? "fresh" : null,
  );
  try {
    transaction(() => {
      guard();
      guard();
      expect(() =>
        transaction(() => {
          database.exec("UPDATE session_nodes SET writer = 'revoked'");
          expect(guard).toThrow("Prepared session entry facts are no longer current");
          throw new Error("rollback nested write");
        }),
      ).toThrow("rollback nested write");
      expect(guard).not.toThrow();
      expect(sql.counts.fresh).toBe(1);
    });
    expect(guard).not.toThrow();
    expect(sql.counts.fresh).toBe(2);
    release();
    expect(guard).toThrow("source released");
  } finally {
    sql.restore();
  }
});

it("observes a foreign process commit after read admission before the next transaction", () => {
  const filename = path.join(tempDirs.make("session-revision-foreign-"), "agent.sqlite");
  const { database, guard, transaction } = fixture(filename);
  runSqliteReadOperationSync(database, () => {
    // The old unpinned scope cannot certify the snapshot acquired by the later BEGIN.
    execFileSync(process.execPath, [
      "--input-type=module",
      "-e",
      "import { DatabaseSync } from 'node:sqlite'; const db = new DatabaseSync(process.argv[1]); db.exec(\"UPDATE session_nodes SET writer = 'foreign'\"); db.close();",
      filename,
    ]);
    expect(() => transaction(guard)).toThrow("Prepared session entry facts are no longer current");
  });
  expect(guard).toThrow("Prepared session entry facts are no longer current");
});
