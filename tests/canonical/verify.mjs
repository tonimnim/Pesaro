// Run from any directory: node tests/canonical/verify.mjs
// Expectations are read-only checked-in bytes, never recomputed into the corpus.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import protobuf from 'protobufjs';
import { canonical, digest, parse } from './canonical.mjs';
import { binaryRequest, claims, jsonRequest, material, typeFor } from './ledger.mjs';

const corpus = JSON.parse(readFileSync(new URL('../../contracts/ledger/v1/testdata/canonical-v1.json', import.meta.url), 'utf8'));
assert.equal(corpus.schema_version, '1');
assert.equal(corpus.profile, 'pesaro.ledger/canonical-v1');
const names = new Set();
function check(id, run) {
  assert(!names.has(id), `duplicate fixture ${id}`);
  names.add(id);
  try { run(); } catch (e) { throw Error(`Fixture ${id}: ${e.message}`, { cause: e }); }
}
function golden(actual, expected, sha256) {
  assert.equal(actual, expected, 'canonical UTF-8 bytes');
  assert.equal(digest(actual), sha256, 'SHA-256');
}
function reversedFields(bytes) {
  const reader = protobuf.Reader.create(bytes), fields = [];
  while (reader.pos < reader.len) {
    const start = reader.pos;
    reader.skipType(reader.uint32() & 7);
    fields.unshift(bytes.subarray(start, reader.pos));
  }
  return Buffer.concat(fields);
}
const byID = new Map(corpus.commands.map(v => [v.id, v]));
assert.equal(byID.size, corpus.commands.length);
assert.equal(new Set(corpus.commands.map(v => v.method)).size, 7);
for (const v of corpus.commands) check('command/' + v.id, () => {
  const fromJSON = jsonRequest(v.method, v.request_json);
  const wire = Buffer.from(v.protobuf_hex, 'hex');
  assert(wire.length > 0);
  const reversed = reversedFields(wire);
  assert.notDeepEqual(wire, reversed, 'different binary field order');
  const encoded = typeFor(v.method).encode(typeFor(v.method).fromObject(fromJSON)).finish();
  // Compare meaning, never require a serializer's preferred byte order.
  for (const decoded of [fromJSON, binaryRequest(v.method, wire), binaryRequest(v.method, reversed), binaryRequest(v.method, encoded)]) {
    golden(material(v.method, decoded), v.expected_canonical, v.expected_sha256);
  }
  if (v.same_as) {
    assert.equal(v.expected_canonical, byID.get(v.same_as).expected_canonical);
    assert.equal(v.expected_sha256, byID.get(v.same_as).expected_sha256);
  }
  if (v.different_from) {
    assert.notEqual(v.expected_canonical, byID.get(v.different_from).expected_canonical);
    assert.notEqual(v.expected_sha256, byID.get(v.different_from).expected_sha256);
  }
});
for (const v of corpus.rejected) check('reject/' + v.id, () => {
  assert(v.request_json || v.protobuf_hex, 'empty rejection');
  if (v.request_json) assert.throws(() => material(v.method, jsonRequest(v.method, v.request_json)));
  if (v.protobuf_hex) assert.throws(() => material(v.method, binaryRequest(v.method, Buffer.from(v.protobuf_hex, 'hex'))));
});
for (const v of corpus.jcs) check('jcs/' + v.id, () => {
  if (v.reject) assert.throws(() => canonical(parse(v.input)));
  else golden(canonical(parse(v.input)), v.expected_canonical, v.expected_sha256);
});

const seed = Buffer.from(corpus.test_only_ed25519_seed_hex, 'hex');
assert.equal(seed.length, 32);
const privateKey = createPrivateKey({ key: Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed]), format: 'der', type: 'pkcs8' });
const publicKey = createPublicKey(privateKey);
assert.equal(publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex'), corpus.public_key_hex);
for (const v of corpus.proofs) check('proof/' + v.id, () => {
  assert(['grant', 'evidence'].includes(v.purpose));
  const normalized = claims(v.purpose, parse(v.claims_json));
  const message = 'pesaro.ledger/' + v.purpose + '/v1\n' + canonical(normalized);
  golden(message, v.expected_message, v.expected_sha256);
  const signature = sign(null, Buffer.from(message), privateKey);
  assert.equal(signature.toString('base64url'), v.signature);
  assert(verify(null, Buffer.from(message), publicKey, Buffer.from(v.signature, 'base64url')));
  const changed = { ...normalized, principal: (BigInt(normalized.principal) + 1n).toString() };
  assert(!verify(null, Buffer.from('pesaro.ledger/' + v.purpose + '/v1\n' + canonical(changed)), publicKey, signature));
  assert(!verify(null, Buffer.from(message.replace('pesaro.ledger/', 'pesar.ledger/')), publicKey, signature), 'persisted namespace must not follow product renames');
  assert(!verify(null, Buffer.from(message.replace('/' + v.purpose + '/', '/' + (v.purpose === 'grant' ? 'evidence' : 'grant') + '/')), publicKey, signature), 'proof purpose separation');
});
for (const group of ['commands', 'rejected', 'jcs', 'proofs']) assert(corpus[group].length > 0);
console.log(`T-18 JavaScript: ${corpus.commands.length} commands (JSON + frozen/reordered/re-encoded Protobuf), ${corpus.rejected.length} rejections, ${corpus.jcs.length} JCS vectors, ${corpus.proofs.length} signed proofs passed on ${process.version}.`);
