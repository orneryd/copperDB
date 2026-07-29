import * as neo4j from "neo4j-driver/lib/browser/neo4j-web.js";

const uri = process.argv[2];
if (!uri) {
  throw new Error("usage: node bolt-websocket-driver-e2e.mjs <bolt-uri>");
}

const driver = neo4j.driver(uri, neo4j.auth.basic("neo4j", "password"), {
  fetchSize: 1,
});
let session;
try {
  session = driver.session();
  const result = await session.run("RETURN 1 AS value");
  if (result.records.length !== 3) {
    throw new Error(`expected 3 records, received ${result.records.length}`);
  }
  if (result.records[0].get("value").toNumber() !== 1) {
    throw new Error("expected the first record to contain value 1");
  }

  const transaction = session.beginTransaction();
  const transactionResult = await transaction.run("RETURN 1 AS value");
  if (transactionResult.records.length !== 3) {
    throw new Error(
      `expected 3 transaction records, received ${transactionResult.records.length}`,
    );
  }
  await transaction.commit();
} finally {
  if (session) {
    await session.close();
  }
  await driver.close();
}
