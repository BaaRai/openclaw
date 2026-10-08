import path from "node:path";

/** SDK and native package discovery share the same bounded lexical parent walk. */
export function* pluginPackageAncestors(startDir: string): Generator<string> {
  let cursor = path.resolve(startDir);
  for (let depth = 0; depth < 12; depth += 1) {
    yield cursor;
    const parent = path.dirname(cursor);
    if (parent === cursor) {
      break;
    }
    cursor = parent;
  }
}
