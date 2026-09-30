// Test-only independent RFC 8785 consumer. No Go subprocess or production helpers.
// Financial integers remain strings; JS numbers are used only by the JCS vectors.
import { createHash } from 'node:crypto';

export function parse(text) {
  if (Buffer.byteLength(text) > 65536) throw Error('JSON size');
  let i = 0;
  const ws = () => { while (/[\t\n\r ]/.test(text[i] ?? '\0')) i++; };
  function string() {
    const start = i++;
    while (i < text.length) {
      if (text[i++] === '"') {
        const value = JSON.parse(text.slice(start, i));
        if (!value.isWellFormed()) throw Error('Unicode');
        return value;
      }
      if (text[i - 1] === '\\') i++;
    }
    throw Error('unterminated string');
  }
  function value(depth = 0) {
    if (depth > 100) throw Error('JSON depth');
    ws();
    if (text[i] === '"') return string();
    if (text[i] === '{') {
      i++;
      const out = Object.create(null);
      ws();
      if (text[i] === '}') { i++; return out; }
      while (true) {
        ws();
        if (text[i] !== '"') throw Error('object key');
        const key = string();
        if (Object.hasOwn(out, key)) throw Error('duplicate key');
        ws();
        if (text[i++] !== ':') throw Error('colon');
        out[key] = value(depth + 1);
        ws();
        const next = text[i++];
        if (next === '}') return out;
        if (next !== ',') throw Error('object delimiter');
      }
    }
    if (text[i] === '[') {
      i++;
      const out = [];
      ws();
      if (text[i] === ']') { i++; return out; }
      while (true) {
        out.push(value(depth + 1));
        ws();
        const next = text[i++];
        if (next === ']') return out;
        if (next !== ',') throw Error('array delimiter');
      }
    }
    const token = /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(text.slice(i));
    if (!token) throw Error('JSON value');
    i += token[0].length;
    const out = JSON.parse(token[0]);
    if (typeof out === 'number' && !Number.isFinite(out)) throw Error('nonfinite');
    return out;
  }
  const result = value();
  ws();
  if (i !== text.length) throw Error('trailing JSON');
  return result;
}

export function canonical(value) {
  if (value === null || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'string') {
    if (!value.isWellFormed()) throw Error('Unicode');
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw Error('nonfinite');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (typeof value !== 'object') throw Error('unsupported value');
  // Emit directly: rebuilding an object would reorder integer-like keys.
  // JS sort compares UTF-16 code units, as required by RFC 8785 section 3.2.3.
  return '{' + Object.keys(value).sort().map(key => canonical(key) + ':' + canonical(value[key])).join(',') + '}';
}

export const digest = text => createHash('sha256').update(text, 'utf8').digest('hex');
