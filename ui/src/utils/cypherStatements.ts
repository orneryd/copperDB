export function splitCypherStatements(script: string): string[] {
  const out: string[] = [];
  let start = 0;
  let i = 0;

  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < script.length) {
    const ch = script[i];
    const next = i + 1 < script.length ? script[i + 1] : "";

    if (inLineComment) {
      if (ch === "\n") {
        inLineComment = false;
      }
      i++;
      continue;
    }

    if (inBlockComment) {
      if (ch === "*" && next === "/") {
        inBlockComment = false;
        i += 2;
        continue;
      }
      i++;
      continue;
    }

    if (!inSingle && !inDouble && !inBacktick) {
      if (ch === "/" && next === "/") {
        inLineComment = true;
        i += 2;
        continue;
      }
      if (ch === "-" && next === "-") {
        inLineComment = true;
        i += 2;
        continue;
      }
      if (ch === "/" && next === "*") {
        inBlockComment = true;
        i += 2;
        continue;
      }
    }

    if (!inDouble && !inBacktick && ch === "'") {
      // Cypher escapes single quotes as '' inside single-quoted strings.
      if (inSingle && next === "'") {
        i += 2;
        continue;
      }
      inSingle = !inSingle;
      i++;
      continue;
    }

    if (!inSingle && !inBacktick && ch === '"') {
      if (inDouble && next === '"') {
        i += 2;
        continue;
      }
      inDouble = !inDouble;
      i++;
      continue;
    }

    if (!inSingle && !inDouble && ch === "`") {
      if (inBacktick && next === "`") {
        i += 2;
        continue;
      }
      inBacktick = !inBacktick;
      i++;
      continue;
    }

    if (!inSingle && !inDouble && !inBacktick && ch === ";") {
      const stmt = script.slice(start, i).trim();
      if (stmt.length > 0) {
        out.push(stmt);
      }
      start = i + 1;
    }

    i++;
  }

  const tail = script.slice(start).trim();
  if (tail.length > 0) {
    out.push(tail);
  }

  return out;
}

