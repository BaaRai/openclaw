// Explicit release-cell fault injection; never imports or rewrites product modules.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import sqlite, { DatabaseSync } from "node:sqlite";
import { inspect } from "node:util";
import { isMainThread, parentPort, threadId } from "node:worker_threads";
import {
  assertWorkerCellPackageIdentity,
  readWorkerCellPackageIdentity,
} from "./worker-cell-package.mjs";

const fixtureName = "snapshot-cleanup-fixture.json";
const refusal = "SQLite artifact-preserving copy and cleanup failed";
const denial = "snapshot cleanup refusal fixture";
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const readJson = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
const writeJson = (file, value) =>
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });

function family(source) {
  return Object.fromEntries(
    ["", "-wal", "-shm", "-journal"].flatMap((suffix) => {
      const file = source + suffix;
      return fs.existsSync(file) ? [[suffix, hash(fs.readFileSync(file))]] : [];
    }),
  );
}

function processIdentity(entrypoint) {
  if (!entrypoint || !fs.existsSync(entrypoint)) {
    return undefined;
  }
  const file = fs.realpathSync(entrypoint);
  for (let root = path.dirname(file), depth = 0; depth < 5; root = path.dirname(root), depth++) {
    const manifest = path.join(root, "package.json");
    const build = path.join(root, "dist/build-info.json");
    if (fs.existsSync(manifest) && fs.existsSync(build) && readJson(manifest).name === "openclaw") {
      const info = readJson(build);
      return {
        root,
        version: info.version,
        commit: info.commit,
        entrypoint: path.relative(root, file),
        entrypointSha256: hash(fs.readFileSync(file)),
        manifestSha256: hash(fs.readFileSync(manifest)),
        buildInfoSha256: hash(fs.readFileSync(build)),
      };
    }
  }
  return undefined;
}

export function seedSnapshotCleanupRefusal({
  source,
  artifacts,
  candidateIdentity,
  baseline,
  gatewayPid,
}) {
  assert.equal(baseline.version, "2026.9.7");
  const candidateCommit = candidateIdentity.buildInfo.commit;
  assert.match(candidateCommit, /^[0-9a-f]{40}$/u);
  assert(Number.isSafeInteger(gatewayPid) && gatewayPid > 1);
  const database = new DatabaseSync(source);
  try {
    database.exec(
      "CREATE TABLE snapshot_cleanup_witness(value INTEGER NOT NULL); INSERT INTO snapshot_cleanup_witness VALUES(0)",
    );
  } finally {
    database.close();
  }
  writeJson(path.join(artifacts, "snapshot-cleanup-candidate-identity.json"), candidateIdentity);
  writeJson(path.join(artifacts, fixtureName), { source, candidateCommit, baseline, gatewayPid });
}

function doctorAncestor(artifacts) {
  for (let pid = process.pid, depth = 0; pid > 1 && depth < 12; depth++) {
    const file = path.join(artifacts, "snapshot-cleanup-doctor-" + pid + ".json");
    if (fs.existsSync(file)) {
      return readJson(file);
    }
    try {
      const stat = fs.readFileSync("/proc/" + pid + "/stat", "utf8");
      pid = Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1]);
    } catch {
      return undefined;
    }
  }
  return undefined;
}

// Native online backup bypasses descriptor copy. Observe its native boundary
// without changing source admission, arguments, completion, or cleanup.
export function observeSnapshotNativeBackups(sourcePath, onAcquisition) {
  const backup = sqlite.backup;
  sqlite.backup = (source, destination, ...args) => {
    if (source.location() === sourcePath) {
      onAcquisition(path.dirname(String(destination)));
    }
    return backup(source, destination, ...args);
  };
  syncBuiltinESMExports();
  return () => {
    sqlite.backup = backup;
    syncBuiltinESMExports();
  };
}

function installFault() {
  const artifacts = process.env.OPENCLAW_UPGRADE_SURVIVOR_ARTIFACT_ROOT;
  if (!artifacts || !fs.existsSync(path.join(artifacts, fixtureName))) {
    return;
  }
  assert.equal(process.platform, "linux");
  assert(fs.existsSync("/.dockerenv"), "Snapshot fault injection requires disposable Docker state");
  const fixture = readJson(path.join(artifacts, fixtureName));
  const identity = processIdentity(process.argv[1]);
  if (!identity) {
    return;
  }
  const role = process.argv[2] === "--doctor" ? "doctor" : process.argv[2];
  if (isMainThread && role === "update" && identity.version === fixture.baseline.version) {
    writeJson(path.join(artifacts, "snapshot-cleanup-driver.json"), { pid: process.pid, identity });
  }
  // The old driver's own capture is deliberately untouched. A candidate Doctor
  // (and its real inspection subprocesses) must reach the repaired producer.
  if (identity.commit !== fixture.candidateCommit) {
    return;
  }
  const expected = readJson(path.join(artifacts, "snapshot-cleanup-candidate-identity.json"));
  assert.equal(identity.entrypointSha256, expected.files[identity.entrypoint]?.sha256);
  assert.equal(identity.manifestSha256, expected.files["package.json"]?.sha256);
  assert.equal(identity.buildInfoSha256, expected.files["dist/build-info.json"]?.sha256);
  identity.payloadSha256 = hash(JSON.stringify(expected));
  if (isMainThread && role === "doctor") {
    assertWorkerCellPackageIdentity(readWorkerCellPackageIdentity(identity.root), expected);
    writeJson(path.join(artifacts, "snapshot-cleanup-doctor-" + process.pid + ".json"), {
      pid: process.pid,
      parentPid: process.ppid,
      identity,
      updateInProgress: process.env.OPENCLAW_UPDATE_IN_PROGRESS === "1",
      fullPayloadVerified: true,
    });
  }
  const descriptors = new Map();
  const open = fs.openSync.bind(fs);
  const close = fs.closeSync.bind(fs);
  const sync = fs.fsyncSync.bind(fs);
  const read = fs.readSync.bind(fs);
  const remove = fs.rmSync.bind(fs);
  const removeAsync = fs.promises.rm.bind(fs.promises);
  const allocate = fs.mkdtempSync.bind(fs);
  let lastAllocation;
  const attempts = new Set();
  const attemptsReceipt = path.join(
    artifacts,
    "snapshot-cleanup-attempts-" + process.pid + "-" + threadId + ".json",
  );
  const recordAcquisition = (directory) => {
    attempts.add(directory);
    writeJson(attemptsReceipt, {
      pid: process.pid,
      threadId,
      identity,
      directories: [...attempts],
    });
  };
  observeSnapshotNativeBackups(fixture.source, recordAcquisition);
  let observed;
  let injecting = false;
  let transcript = "";
  const receipt = path.join(
    artifacts,
    "snapshot-cleanup-copy-" + process.pid + "-" + threadId + ".json",
  );
  const save = () => writeJson(receipt, observed);
  const owns = (file) =>
    observed &&
    (path.resolve(String(file)) === observed.staging ||
      path.resolve(String(file)).startsWith(observed.staging + path.sep));
  const inject = () => {
    if (observed || injecting) {
      return;
    }
    const stack = new Error().stack ?? "";
    if (!stack.includes("prepareArtifactPreservingCopy")) {
      return;
    }
    injecting = true;
    try {
      const doctor = doctorAncestor(artifacts);
      if (!doctor?.updateInProgress) {
        return;
      }
      try {
        process.kill(fixture.gatewayPid, 0);
        return;
      } catch (error) {
        if (error.code !== "ESRCH") {
          throw error;
        }
      }
      const target = [...descriptors.values()].find(
        (file) =>
          /(?:^|\/)(?:first|database\.sqlite\.partial)$/u.test(file) &&
          path.basename(path.dirname(file)).startsWith("openclaw-sqlite-readonly-") &&
          fs.existsSync(file) &&
          fs.statSync(file).size > 0,
      );
      if (!target || ![...descriptors.values()].includes(fixture.source)) {
        return;
      }
      try {
        fs.writeFileSync(
          path.join(artifacts, "snapshot-cleanup-fault-claim"),
          String(process.pid),
          { flag: "wx", mode: 0o600 },
        );
      } catch (error) {
        if (error.code === "EEXIST") {
          return;
        }
        throw error;
      }
      const writer = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          "import {DatabaseSync} from 'node:sqlite'; const db=new DatabaseSync(process.argv[1]); db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE); UPDATE snapshot_cleanup_witness SET value=value+1;'); process.kill(process.pid, 'SIGKILL');",
          fixture.source,
        ],
        { env: { ...process.env, NODE_OPTIONS: "" }, encoding: "utf8", timeout: 30000 },
      );
      assert.equal(
        writer.signal,
        "SIGKILL",
        "Native fixture writer did not commit and settle: " + writer.stderr,
      );
      observed = {
        pid: process.pid,
        threadId,
        parentPid: process.ppid,
        identity,
        doctor,
        writer: { pid: writer.pid, signal: writer.signal },
        staging: path.dirname(target),
        afterWriter: family(fixture.source),
        cleanupDenials: 0,
        terminalRefusal: false,
        sourcePreservedAtRefusal: false,
        cleanupOwnerObserved: false,
        removed: false,
      };
      save();
    } finally {
      injecting = false;
    }
  };
  fs.openSync = (file, flags, mode) => {
    const fd = open(file, flags, mode);
    const pathname = path.resolve(String(file));
    descriptors.set(fd, pathname);
    // Both copy adapters synchronously reacquire the source header just after
    // allocation, before yielding to byte-copy or native CoW. Observe that
    // boundary in every isolate, even after failure serialization or handoff.
    if (
      pathname === fixture.source &&
      (new Error().stack ?? "").includes("createStableReadOnlyCopyInTempDirectory")
    ) {
      assert(lastAllocation, "Capture source was opened without observed allocation");
      recordAcquisition(lastAllocation);
    }
    return fd;
  };
  fs.closeSync = (fd) => {
    try {
      return close(fd);
    } finally {
      descriptors.delete(fd);
    }
  };
  fs.fsyncSync = (fd) => {
    const result = sync(fd);
    inject();
    return result;
  };
  fs.readSync = (...args) => {
    const result = read(...args);
    inject();
    return result;
  };
  fs.mkdtempSync = (...args) => {
    const directory = allocate(...args);
    if (path.basename(directory).startsWith("openclaw-sqlite-readonly-")) {
      lastAllocation = directory;
    }
    return directory;
  };
  const beforeRemove = (file) => {
    if (!owns(file)) {
      return;
    }
    if (!observed.terminalRefusal) {
      observed.cleanupDenials++;
      save();
      throw Object.assign(new Error(denial), { code: "EACCES", path: String(file) });
    }
    observed.cleanupOwnerObserved ||= /removeTempDirectory|retireSqliteSnapshotPayload/u.test(
      new Error().stack ?? "",
    );
  };
  const afterRemove = (file) => {
    if (owns(file)) {
      observed.removed = !fs.existsSync(observed.staging);
      save();
    }
  };
  fs.rmSync = (file, options) => {
    beforeRemove(file);
    const result = remove(file, options);
    afterRemove(file);
    return result;
  };
  fs.promises.rm = async (file, options) => {
    beforeRemove(file);
    await removeAsync(file, options);
    afterRemove(file);
  };
  const observeTerminal = (data) => {
    if (observed && !observed.terminalRefusal) {
      transcript = (transcript + data).slice(-32768);
      if (
        transcript.includes(refusal) &&
        /SQLite (?:journal state|journal mode|WAL generation|WAL|main database).*changed/u.test(
          transcript,
        )
      ) {
        observed.terminalRefusal = true;
        observed.unpublishedAtRefusal = !fs.existsSync(
          path.join(observed.staging, "database.sqlite"),
        );
        observed.sourcePreservedAtRefusal =
          JSON.stringify(family(fixture.source)) === JSON.stringify(observed.afterWriter);
        save();
      }
    }
  };
  for (const stream of [process.stdout, process.stderr]) {
    const write = stream.write.bind(stream);
    stream.write = (...args) => {
      observeTerminal(String(args[0]));
      return write(...args);
    };
  }
  const observeMessage = (value) => {
    if (observed && !observed.terminalRefusal) {
      observeTerminal(
        inspect(value, {
          depth: 8,
          customInspect: false,
          getters: false,
          maxArrayLength: 20,
          maxStringLength: 32768,
        }),
      );
    }
  };
  if (process.send) {
    const send = process.send.bind(process);
    process.send = (...args) => {
      observeMessage(args[0]);
      return send(...args);
    };
  }
  if (parentPort) {
    const postMessage = parentPort.postMessage.bind(parentPort);
    parentPort.postMessage = (value, ...args) => {
      observeMessage(value);
      return postMessage(value, ...args);
    };
  }
  syncBuiltinESMExports();
}

export function writeSnapshotCleanupEvidence(artifacts) {
  // Capture even when the candidate fault never ran; missing observations are
  // diagnostic evidence, not a passed proof. The host redacts the projection.
  const names = fs
    .readdirSync(artifacts)
    .filter((name) =>
      /^snapshot-cleanup-(?:fixture|candidate|driver|doctor-\d+|attempts-\d+-\d+|copy-\d+-\d+)\.json$/u.test(
        name,
      ),
    );
  assert(names.length <= 128, "Snapshot evidence exceeds the diagnostic collection bound");
  writeJson(
    path.join(artifacts, "snapshot-cleanup-evidence.json"),
    Object.fromEntries(names.map((name) => [name, readJson(path.join(artifacts, name))])),
  );
}

export function assertSnapshotCleanupRefusal(artifacts, updateResult) {
  const fixture = readJson(path.join(artifacts, fixtureName));
  const driver = readJson(path.join(artifacts, "snapshot-cleanup-driver.json"));
  assert.equal(driver.identity.commit, fixture.baseline.commit);
  const receipts = fs
    .readdirSync(artifacts)
    .filter((file) => /^snapshot-cleanup-copy-\d+-\d+\.json$/u.test(file))
    .map((file) => readJson(path.join(artifacts, file)));
  assert.equal(
    receipts.length,
    1,
    "Expected one candidate-owned fault, not an old-driver or direct helper call",
  );
  const observed = receipts[0];
  assert.equal(observed.identity.commit, fixture.candidateCommit);
  assert.equal(observed.doctor.identity.commit, fixture.candidateCommit);
  assert.equal(observed.doctor.updateInProgress, true);
  assert.equal(observed.doctor.fullPayloadVerified, true);
  const expectedPayload = hash(
    JSON.stringify(readJson(path.join(artifacts, "snapshot-cleanup-candidate-identity.json"))),
  );
  assert.equal(observed.identity.payloadSha256, expectedPayload);
  assert.equal(observed.doctor.identity.payloadSha256, expectedPayload);
  assert(observed.cleanupDenials > 0, "No real copy cleanup was refused");
  const acquisitions = fs
    .readdirSync(artifacts)
    .filter((file) => /^snapshot-cleanup-attempts-\d+-\d+\.json$/u.test(file))
    .flatMap((file) => {
      const attempts = readJson(path.join(artifacts, file));
      assert.equal(attempts.identity.commit, fixture.candidateCommit);
      assert.equal(attempts.identity.payloadSha256, expectedPayload);
      return attempts.directories;
    });
  assert.deepEqual(
    [...new Set(acquisitions)],
    [observed.staging],
    "Candidate reacquired the source before updater settlement, possibly in another process",
  );
  assert.equal(
    observed.terminalRefusal,
    true,
    "Candidate did not surface terminal aggregate refusal",
  );
  assert.equal(
    observed.sourcePreservedAtRefusal,
    true,
    "Capture changed the source after the fixture writer",
  );
  assert.equal(
    observed.unpublishedAtRefusal,
    true,
    "The failed attempt was published as a snapshot",
  );
  assert(
    Number.isInteger(updateResult.exitCode) && updateResult.exitCode > 0,
    "Updater swallowed the copy refusal and returned success",
  );
  assert.equal(updateResult.signal, null, "Updater was terminated instead of reporting refusal");
  const stdout = fs.readFileSync(path.join(artifacts, "update.stdout"), "utf8");
  const result = JSON.parse(stdout.slice(stdout.indexOf("{")));
  assert.equal(result.status, "error", "Updater did not publish a failed result");
  const publicOutput = stdout + fs.readFileSync(path.join(artifacts, "update.stderr"), "utf8");
  assert(publicOutput.includes(refusal), "Published updater omitted the candidate capture refusal");
  const source = new DatabaseSync(fixture.source, { readOnly: true });
  try {
    assert.equal(
      source.prepare("SELECT value FROM snapshot_cleanup_witness").get().value,
      1,
      "Updater lost the fixture writer commit after refusal",
    );
    assert.equal(source.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  } finally {
    source.close();
  }
  assert.equal(observed.cleanupOwnerObserved, true, "Retained cleanup owner was not observed");
  assert.equal(observed.removed, true, "Retained cleanup did not safely remove the failed attempt");
  assert(
    !fs.existsSync(observed.staging),
    "Unpublished staging still exists after updater settlement",
  );
  const proof = {
    status: "passed",
    baseline: fixture.baseline,
    candidateCommit: fixture.candidateCommit,
    updateExit: updateResult.exitCode,
    acquisitions,
    fault: observed,
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    limitation:
      "Synthetic SQLite writer commits then exits by SIGKILL to retain WAL; filesystem EACCES is injected in a Linux container. Not Darwin or large-data proof.",
  };
  writeJson(path.join(artifacts, "snapshot-cleanup-proof.json"), proof);
  console.log("SNAPSHOT_CLEANUP_REFUSAL " + JSON.stringify(proof));
}

installFault();
