/*
 * Dataflow UDF for the Google-provided Pub/Sub Subscription to BigQuery template.
 * Validates each ride event and adds ingest_ts. A thrown error sends the message
 * to the template's dead-letter table instead of the raw table.
 *
 * ES5 only: the classic template runs UDFs in Nashorn (no let/const/arrow functions).
 * Rules mirror producer/schema.py; change both together.
 */
var STRING_FIELDS = ["ride_id", "driver_id", "rider_id", "pickup_zone", "dropoff_zone"];
var TIMESTAMP_FIELDS = ["pickup_ts", "dropoff_ts"];
var ALL_FIELDS = STRING_FIELDS.concat(TIMESTAMP_FIELDS, ["fare", "surge_multiplier"]);
var ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
var TS_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$/;
var FARE_MAX = 1000;
var SURGE_MIN = 1;
var SURGE_MAX = 5;
var MAX_TRIP_SECONDS = 21600;

function isNumber(v) {
  return typeof v === "number" && isFinite(v);
}

function parseTs(v) {
  var m = TS_RE.exec(v);
  if (!m) { return null; }
  var ms = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
  var d = new Date(ms);
  if (d.getUTCFullYear() !== +m[1] || d.getUTCMonth() !== +m[2] - 1 || d.getUTCDate() !== +m[3] ||
      d.getUTCHours() !== +m[4] || d.getUTCMinutes() !== +m[5] || d.getUTCSeconds() !== +m[6]) {
    return null;
  }
  return ms;
}

function validate(row) {
  var problems = [];
  var i, name, value;
  if (row === null || typeof row !== "object" || Array.isArray(row)) {
    return ["<root>: payload must be a JSON object"];
  }
  for (name in row) {
    if (Object.prototype.hasOwnProperty.call(row, name) && ALL_FIELDS.indexOf(name) < 0) {
      problems.push(name + ": UNKNOWN_FIELD");
    }
  }
  for (i = 0; i < ALL_FIELDS.length; i++) {
    name = ALL_FIELDS[i];
    if (row[name] === undefined || row[name] === null) { problems.push(name + ": MISSING_FIELD"); }
  }
  for (i = 0; i < STRING_FIELDS.length; i++) {
    name = STRING_FIELDS[i];
    value = row[name];
    if (value === undefined || value === null) { continue; }
    if (typeof value !== "string") { problems.push(name + ": BAD_TYPE"); }
    else if (!ID_RE.test(value)) { problems.push(name + ": BAD_ID"); }
  }
  var parsed = {};
  for (i = 0; i < TIMESTAMP_FIELDS.length; i++) {
    name = TIMESTAMP_FIELDS[i];
    value = row[name];
    if (value === undefined || value === null) { continue; }
    var ms = typeof value === "string" ? parseTs(value) : null;
    if (ms === null) { problems.push(name + ": BAD_TIMESTAMP"); } else { parsed[name] = ms; }
  }
  if (parsed.pickup_ts !== undefined && parsed.dropoff_ts !== undefined) {
    var seconds = (parsed.dropoff_ts - parsed.pickup_ts) / 1000;
    if (seconds < 0) { problems.push("dropoff_ts: DROPOFF_BEFORE_PICKUP"); }
    else if (seconds > MAX_TRIP_SECONDS) { problems.push("dropoff_ts: TRIP_TOO_LONG"); }
  }
  if (row.fare !== undefined && row.fare !== null) {
    if (!isNumber(row.fare)) { problems.push("fare: BAD_TYPE"); }
    else if (!(row.fare > 0 && row.fare <= FARE_MAX)) { problems.push("fare: FARE_OUT_OF_RANGE"); }
  }
  if (row.surge_multiplier !== undefined && row.surge_multiplier !== null) {
    if (!isNumber(row.surge_multiplier)) { problems.push("surge_multiplier: BAD_TYPE"); }
    else if (row.surge_multiplier < SURGE_MIN || row.surge_multiplier > SURGE_MAX) {
      problems.push("surge_multiplier: SURGE_OUT_OF_RANGE");
    }
  }
  return problems;
}

function process(inJson) {
  var row = JSON.parse(inJson);
  var problems = validate(row);
  if (problems.length > 0) {
    throw new Error("RIDE_EVENT_INVALID: " + problems.join("; "));
  }
  row.ingest_ts = new Date().toISOString();
  return JSON.stringify(row);
}
