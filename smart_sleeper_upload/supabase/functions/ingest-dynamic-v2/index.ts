import { withSupabase, type SupabaseContext } from "@supabase/server"

const VALID_FILTERING_MODES = new Set(["off", "shadow", "enforce"])
const TIME_FIELDS = ["timestamp", "source_time_utc", "ingest_time_utc"] as const

type JsonObject = Record<string, unknown>

type DynamicPayload = {
  run: JsonObject
  quality_report: JsonObject
  records: JsonObject[]
  anomaly_events: JsonObject[]
  state: {
    state_key: string
    state_json: JsonObject
  }
}

type RpcArgs = {
  p_run_key: string
  p_source_name: string
  p_source_file_name: string
  p_source_type: string
  p_pipeline_version: string
  p_filtering_mode: string
  p_dynamic_config_hash: string | null
  p_state_schema_version: number
  p_quality_report: JsonObject
  p_records: JsonObject[]
  p_anomaly_events: JsonObject[]
  p_state_key: string
  p_dynamic_state: JsonObject
}

type SupabaseAdminLike = {
  rpc: (name: string, args: RpcArgs) => Promise<{ data: unknown; error: unknown }>
  from: (table: string) => {
    select: (columns: string) => {
      eq: (column: string, value: string) => {
        maybeSingle: () => Promise<{ data: unknown; error: unknown }>
      }
    }
  }
}

export function canonicalTimestamp(record: JsonObject): string | null {
  for (const field of TIME_FIELDS) {
    const value = record[field]
    if (typeof value !== "string" || value.trim() === "") {
      continue
    }
    const parsed = new Date(value)
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString()
    }
  }
  return null
}

export function validateIngestPayload(value: unknown): DynamicPayload {
  if (!isObject(value)) {
    throw new ValidationError("request body must be a JSON object")
  }
  const run = value.run
  if (!isObject(run)) {
    throw new ValidationError("run object is required")
  }
  requireText(run, "run_key")
  requireText(run, "source_name")
  requireText(run, "source_file_name")
  requireText(run, "source_type")
  requireText(run, "pipeline_version")
  requireText(run, "execution_slot")
  const filteringMode = requireText(run, "filtering_mode")
  if (!VALID_FILTERING_MODES.has(filteringMode)) {
    throw new ValidationError("unsupported filtering mode")
  }
  const stateSchemaVersion = run.state_schema_version
  if (!Number.isInteger(stateSchemaVersion) || Number(stateSchemaVersion) < 1) {
    throw new ValidationError("state_schema_version must be a positive integer")
  }
  if (run.dynamic_config_hash !== null && run.dynamic_config_hash !== undefined) {
    requireText(run, "dynamic_config_hash")
  }

  if (!isObject(value.quality_report)) {
    throw new ValidationError("quality_report must be an object")
  }

  if (!Array.isArray(value.records)) {
    throw new ValidationError("records must be an array")
  }
  const seenRecordIds = new Set<string>()
  for (const [index, record] of value.records.entries()) {
    if (!isObject(record)) {
      throw new ValidationError(`records[${index}] must be an object`)
    }
    const recordId = requireText(record, "record_id")
    if (seenRecordIds.has(recordId)) {
      throw new ValidationError(`duplicate record_id: ${recordId}`)
    }
    seenRecordIds.add(recordId)
    requireText(record, "family")
    if (canonicalTimestamp(record) === null) {
      throw new ValidationError(`record ${recordId} is missing a valid canonical timestamp`)
    }
  }

  if (!Array.isArray(value.anomaly_events)) {
    throw new ValidationError("anomaly_events must be an array")
  }
  for (const [index, event] of value.anomaly_events.entries()) {
    if (!isObject(event)) {
      throw new ValidationError(`anomaly_events[${index}] must be an object`)
    }
    if ("event_id" in event) {
      requireText(event, "event_id")
    }
  }

  if (!isObject(value.state)) {
    throw new ValidationError("state object is required")
  }
  const stateKey = requireText(value.state, "state_key")
  if (!isObject(value.state.state_json)) {
    throw new ValidationError("state.state_json must be an object")
  }

  return {
    run,
    quality_report: value.quality_report,
    records: value.records,
    anomaly_events: value.anomaly_events,
    state: {
      state_key: stateKey,
      state_json: value.state.state_json,
    },
  }
}

export function buildRpcArgs(payload: DynamicPayload): RpcArgs {
  return {
    p_run_key: String(payload.run.run_key),
    p_source_name: String(payload.run.source_name),
    p_source_file_name: String(payload.run.source_file_name),
    p_source_type: String(payload.run.source_type),
    p_pipeline_version: String(payload.run.pipeline_version),
    p_filtering_mode: String(payload.run.filtering_mode),
    p_dynamic_config_hash:
      payload.run.dynamic_config_hash === null || payload.run.dynamic_config_hash === undefined
        ? null
        : String(payload.run.dynamic_config_hash),
    p_state_schema_version: Number(payload.run.state_schema_version),
    p_quality_report: payload.quality_report,
    p_records: payload.records,
    p_anomaly_events: payload.anomaly_events,
    p_state_key: payload.state.state_key,
    p_dynamic_state: payload.state.state_json,
  }
}

export async function handleIngestRequest(
  req: Request,
  ctx: SupabaseContext<unknown> | { supabaseAdmin: SupabaseAdminLike },
): Promise<Response> {
  if (req.method === "GET") {
    return handleGetState(req, ctx.supabaseAdmin as SupabaseAdminLike)
  }
  if (req.method !== "POST") {
    return jsonResponse({ success: false, error: "method_not_allowed" }, 405)
  }

  let parsed: unknown
  try {
    parsed = await req.json()
  } catch {
    return jsonResponse({ success: false, error: "malformed_json" }, 400)
  }

  let payload: DynamicPayload
  try {
    payload = validateIngestPayload(parsed)
  } catch (error) {
    if (error instanceof ValidationError) {
      return jsonResponse({ success: false, error: error.message }, 400)
    }
    return jsonResponse({ success: false, error: "validation_failed" }, 400)
  }

  const { data, error } = await (ctx.supabaseAdmin as SupabaseAdminLike).rpc(
    "ingest_dynamic_v2",
    buildRpcArgs(payload),
  )
  if (error) {
    return jsonResponse({ success: false, error: "rpc_failed" }, 500)
  }

  const safeResponse = validateRpcResponse(data)
  if (!safeResponse.success) {
    return jsonResponse({ success: false, error: "malformed_rpc_response" }, 500)
  }
  return jsonResponse(safeResponse, 200)
}

export const ingestDynamicFetch = withSupabase(
  { auth: "secret" },
  async (req, ctx) => handleIngestRequest(req, ctx),
)

export default {
  fetch: ingestDynamicFetch,
}

async function handleGetState(req: Request, supabaseAdmin: SupabaseAdminLike): Promise<Response> {
  const stateKey = new URL(req.url).searchParams.get("state_key")
  if (stateKey === null || stateKey.trim() === "") {
    return jsonResponse({ success: false, error: "state_key_required" }, 400)
  }
  const { data, error } = await supabaseAdmin
    .from("dynamic_filter_state")
    .select("state_key,state_schema_version,state_json,dynamic_config_hash,version,updated_at")
    .eq("state_key", stateKey)
    .maybeSingle()
  if (error) {
    return jsonResponse({ success: false, error: "state_query_failed" }, 500)
  }
  if (data === null || data === undefined) {
    return jsonResponse({ found: false }, 200)
  }
  if (!isObject(data)) {
    return jsonResponse({ success: false, error: "malformed_state_response" }, 500)
  }
  return jsonResponse(
    {
      found: true,
      state_key: data.state_key,
      state_schema_version: data.state_schema_version,
      state_json: isObject(data.state_json) ? data.state_json : {},
      dynamic_config_hash: data.dynamic_config_hash ?? null,
      version: data.version,
      updated_at: data.updated_at,
    },
    200,
  )
}

function validateRpcResponse(value: unknown): JsonObject {
  if (!isObject(value) || value.success !== true || typeof value.ingest_run_id !== "string") {
    return { success: false }
  }
  return {
    success: true,
    ingest_run_id: value.ingest_run_id,
    run_key: typeof value.run_key === "string" ? value.run_key : "",
    inserted_records: integerOrZero(value.inserted_records),
    updated_records: integerOrZero(value.updated_records),
    skipped_records: integerOrZero(value.skipped_records),
    anomaly_events: integerOrZero(value.anomaly_events),
    state_saved: value.state_saved === true,
  }
}

function integerOrZero(value: unknown): number {
  return Number.isInteger(value) && Number(value) >= 0 ? Number(value) : 0
}

function jsonResponse(body: JsonObject, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  })
}

function requireText(object: JsonObject, field: string): string {
  const value = object[field]
  if (typeof value !== "string" || value.trim() === "") {
    throw new ValidationError(`${field} is required`)
  }
  return value.trim()
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

class ValidationError extends Error {}
