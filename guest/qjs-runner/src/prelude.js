// The deterministic JavaScript environment the runner installs before a
// user script (sandbox spec §3.2 S2). Single source of truth: lib.rs embeds
// it with include_str!, and tests/prelude_determinism.mjs runs it through a
// real engine. Every nondeterministic source a stock engine offers is
// replaced by a function of the host seed.
(() => {
  // Seeded xorshift64*: Math.random becomes a function of the host seed.
  // __seed() returns the u64 seed as a decimal string, losslessly — an f64
  // would collapse seeds that differ below 2^-52 mantissa precision, and the
  // host's seeds span the full 64-bit range.
  const mask = 0xffffffffffffffffn;
  let s = (BigInt(__seed()) & mask) | 1n;
  Math.random = function () {
    s ^= (s << 13n) & mask;
    s ^= s >> 7n;
    s ^= (s << 17n) & mask;
    // xorshift64* output stage, then the 53 high bits to a double in [0, 1).
    const x = (s * 0x2545f4914f6cdd1dn) & mask;
    return Number(x >> 11n) / 9007199254740992;
  };

  // Frozen clock: time does not advance (documented in the tool).
  const EPOCH = 1700000000000;
  const RealDate = Date;
  function FrozenDate(...args) {
    if (args.length === 0) return new RealDate(EPOCH);
    return new RealDate(...args);
  }
  FrozenDate.now = () => EPOCH;
  FrozenDate.prototype = RealDate.prototype;
  FrozenDate.UTC = RealDate.UTC;
  FrozenDate.parse = RealDate.parse;
  globalThis.Date = FrozenDate;

  globalThis.console = {
    log: (...a) => __console_write(a.map(String).join(' ')),
    warn: (...a) => __console_write(a.map(String).join(' ')),
    error: (...a) => __console_write(a.map(String).join(' ')),
    info: (...a) => __console_write(a.map(String).join(' ')),
  };

  globalThis.workspace = {
    read: (path) => __ws_read(String(path)),
    write: (path, content) => __ws_write(String(path), String(content)),
  };

  globalThis.input = __input_json == null ? undefined : JSON.parse(__input_json);
})();
