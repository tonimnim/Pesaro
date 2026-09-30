// Independent, test-only implementation of the frozen synthetic canonical-v1
// command profile. Not a production SDK or an authorization/accounting engine.
import { readFileSync } from 'node:fs';
import protobuf from 'protobufjs';
import { canonical, parse } from './canonical.mjs';

const root = protobuf.parse(readFileSync(new URL('../../contracts/ledger/v1/ledger.proto', import.meta.url), 'utf8'), { keepCase: true }).root;
root.resolveAll();
export const typeFor = method => root.lookupType('pesaro.ledger.v1.' + method + 'Request');

// Unlike fromObject(), this rejects unknown fields and scalar coercion before
// the protobuf encoder sees them. Only the command schema's field kinds apply.
function shape(type, input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw Error('message');
  // protobuf.js preserves this as a non-enumerable property.
  if (Object.hasOwn(input, '$unknowns')) throw Error('unknown protobuf field');
  for (const key of Object.keys(input)) {
    const field = type.fields[key];
    if (!field) throw Error('unknown JSON field');
    const value = input[key];
    if (value === null) continue; // ProtoJSON null means unset.
    if (field.resolvedType) shape(field.resolvedType, value);
    else if (typeof value !== (field.type === 'bool' ? 'boolean' : 'string')) throw Error('scalar type');
  }
}

export function jsonRequest(method, text) {
  const type = typeFor(method), input = parse(text);
  shape(type, input);
  return type.toObject(type.fromObject(input), { defaults: true });
}

export function binaryRequest(method, bytes) {
  const type = typeFor(method), reader = protobuf.Reader.create(bytes);
  reader.discardUnknown = false;
  const message = type.decode(reader);
  shape(type, message); // Includes retained unknowns at every nesting level.
  return type.toObject(message, { defaults: true });
}

function integer(value, max, positive = false) {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value) || BigInt(value) > max || (positive && value === '0')) throw Error('integer');
  return value;
}

const int64 = 9223372036854775807n;
function uuid(value) {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value) || /^0{8}(-0{4}){3}-0{12}$/.test(value)) throw Error('UUID');
  return value.toLowerCase();
}

// Do not round nanoseconds through JS Date. Date is used only for whole-second
// UTC conversion of ReservePayout.expires_at. Signed claim offsets are retained.
function timestamp(text, utc) {
  const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(text);
  if (!m || !Number.isFinite(Date.parse(m[1] + m[3]))) throw Error('timestamp');
  const fraction = (m[2] ?? '').replace(/0+$/, '');
  const suffix = m[3] === '+00:00' || m[3] === '-00:00' ? 'Z' : m[3];
  return (utc ? new Date(m[1] + m[3]).toISOString().slice(0, 19) : m[1]) + (fraction ? '.' + fraction : '') + (utc ? 'Z' : suffix);
}

function normalize(value, path = '') {
  if (value === null) return null;
  const out = {};
  for (const [key, entry] of Object.entries(value)) {
    if (key === 'signature') continue;
    if (entry && typeof entry === 'object') out[key] = normalize(entry, path + '.' + key);
    else if (key === 'id' || key.endsWith('_id')) out[key] = entry === '' ? '' : uuid(entry);
    else if (key === 'principal' || key === 'fee') out[key] = integer(entry, int64);
    else if (key === 'daily_cap') out[key] = integer(entry, 10n ** 38n - 1n);
    else if (key.endsWith('_epoch') || key === 'expected_version') out[key] = integer(entry, int64, key === 'expected_version');
    else if (key === 'not_before' || key === 'expires_at' || key === 'observed_at') out[key] = timestamp(entry, path === '' && key === 'expires_at');
    else out[key] = entry;
  }
  return out;
}

export function claims(purpose, value) {
  // Purpose itself is checked by the proof caller; this normalization path must
  // never apply the hold-expiry UTC rule to a signed grant's expiry.
  return normalize(value, purpose);
}

const kinds = {
  CreateAccount: 'CREATE_ACCOUNT', TransferInternal: 'TRANSFER_INTERNAL',
  ReservePayout: 'RESERVE_PAYOUT', MarkHoldExposed: 'MARK_HOLD_EXPOSED',
  CapturePayout: 'CAPTURE_PAYOUT', ReleasePayout: 'RELEASE_PAYOUT',
  SetAccountControl: 'SET_ACCOUNT_CONTROL',
};

export function material(method, request) {
  const { envelope, ...payload } = request;
  if (!kinds[method] || envelope?.schema_version !== '1') throw Error('envelope');
  uuid(envelope.operation_id);
  const body = normalize(payload);
  if (method === 'CreateAccount') {
    uuid(body.account_id); uuid(body.owner_id);
    if (body.purpose !== 'WALLET') throw Error('purpose');
  }
  if (method === 'TransferInternal' || method === 'ReservePayout') {
    const t = body.terms;
    for (const key of ['payment_id', 'source_id', 'beneficiary_id', 'policy_id', 'quote_id']) uuid(t[key]);
    integer(t.principal, int64, true);
    if (t.currency !== 'KES') throw Error('currency');
    uuid(t.grant.claims.id); uuid(t.grant.claims.subject_id);
    if (!t.grant.claims.issuer || Buffer.byteLength(t.grant.claims.issuer) > 64) throw Error('issuer');
  }
  if (method === 'ReservePayout') for (const key of ['hold_id', 'attempt_id', 'pool_id']) uuid(body[key]);
  if (body.ref) for (const key of ['payment_id', 'hold_id', 'attempt_id']) uuid(body.ref[key]);
  if (method === 'MarkHoldExposed') uuid(body.grant_id);
  if (method === 'CapturePayout') uuid(body.evidence.claims.id);
  if (method === 'ReleasePayout') {
    if (body.reason === 'FINAL_FAILURE') uuid(body.evidence.claims.id);
    else if (!['CANCEL', 'EXPIRY'].includes(body.reason) || body.evidence !== null) throw Error('release');
  }
  if (method === 'SetAccountControl') {
    uuid(body.key.id);
    if (!['SUBJECT', 'ACCOUNT'].includes(body.key.kind) || !body.reason || Buffer.byteLength(body.reason) > 64) throw Error('control');
  }
  const result = canonical({ schema_version: '1', book_id: uuid(envelope.book_id), kind: kinds[method], body });
  if (Buffer.byteLength(result) > 16384) throw Error('material size');
  return result;
}
