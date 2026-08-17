import { describe, expect, it } from "vitest"

import {
  buildRpcArgs,
  canonicalTimestamp,
  handleIngestRequest,
  ingestDynamicFetch,
  validateIngestPayload,
} from "../supabase/functions/ingest-dynamic-v2/index.ts"

function payload(overrides: Record<string, unknown> = {}) {
  return {
    run: {
      run_key: "run-1",
      source_name: "smart_sleeper",
      source_file_name: "source.json",
      source_type: "smart_sleeper",
      pipeline_version: "dynamic-v2",
      execution_slot: "2026-03-01T00:00:00+00:00",
      filtering_mode: "shadow",
      dynamic_config_hash: null,
      state_schema_version: 2,
    },
    quality_report: {},
    records: [
      {
        record_id: "r1",
        family: "environment",
        timestamp: "2026-03-01T00:00:00Z",
      },
    ],
    anomaly_events: [],
    state: { state_key: "state", state_json: { state_schema_version: 2 } },
    ...overrides,
  }
}

function adminMock(options: { rpcData?: unknown; rpcError?: unknown; stateData?: unknown; stateError?: unknown } = {}) {
  const calls: Array<{ name: string; args: unknown }> = []
  return {
    calls,
    client: {
      async rpc(name: string, args: unknown) {
        calls.push({ name, args })
        return {
          data:
            options.rpcData ??
            {
              success: true,
              ingest_run_id: "00000000-0000-0000-0000-000000000001",
              run_key: "run-1",
              inserted_records: 1,
              updated_records: 0,
              skipped_records: 0,
              anomaly_events: 0,
              state_saved: true,
            },
          error: options.rpcError ?? null,
        }
      },
      from(table: string) {
        return {
          select(columns: string) {
            return {
              eq(column: string, value: string) {
                calls.push({ name: `from:${table}`, args: { columns, column, value } })
                return {
                  async maybeSingle() {
                    return { data: options.stateData ?? null, error: options.stateError ?? null }
                  },
                }
              },
            }
          },
        }
      },
    },
  }
}

async function readJson(response: Response) {
  return JSON.parse(await response.text())
}

describe("ingest dynamic validation", () => {
  it("uses the canonical timestamp priority", () => {
    expect(
      canonicalTimestamp({
        source_time_utc: "2026-03-01T00:00:01Z",
        ingest_time_utc: "2026-03-01T00:00:02Z",
      }),
    ).toBe("2026-03-01T00:00:01.000Z")
  })

  it("builds the fixed RPC argument contract", () => {
    const args = buildRpcArgs(validateIngestPayload(payload()))

    expect(args.p_run_key).toBe("run-1")
    expect(args.p_records).toHaveLength(1)
    expect(args.p_state_key).toBe("state")
    expect(args.p_dynamic_state).toEqual({ state_schema_version: 2 })
  })

  it("rejects malformed and duplicate records", () => {
    expect(() => validateIngestPayload({})).toThrow("run object")
    expect(() => validateIngestPayload(payload({ run: { ...payload().run, filtering_mode: "bad" } }))).toThrow(
      "unsupported",
    )
    expect(() =>
      validateIngestPayload(
        payload({
          records: [
            { record_id: "r1", family: "environment", timestamp: "2026-03-01T00:00:00Z" },
            { record_id: "r1", family: "environment", timestamp: "2026-03-01T00:00:01Z" },
          ],
        }),
      ),
    ).toThrow("duplicate")
    expect(() =>
      validateIngestPayload(payload({ records: [{ record_id: "r1", family: "environment" }] })),
    ).toThrow("canonical timestamp")
  })
})

describe("ingest dynamic handler", () => {
  it("uses secret-auth wrapped fetch export", () => {
    expect(typeof ingestDynamicFetch).toBe("function")
  })

  it("handles a valid POST through admin RPC only", async () => {
    const mock = adminMock()
    const response = await handleIngestRequest(
      new Request("https://example.test", {
        method: "POST",
        body: JSON.stringify(payload()),
      }),
      { supabaseAdmin: mock.client },
    )
    const body = await readJson(response)

    expect(response.status).toBe(200)
    expect(body.success).toBe(true)
    expect(mock.calls[0].name).toBe("ingest_dynamic_v2")
  })

  it("returns safe validation and RPC errors", async () => {
    const malformed = await handleIngestRequest(
      new Request("https://example.test", { method: "POST", body: "{" }),
      { supabaseAdmin: adminMock().client },
    )
    expect(malformed.status).toBe(400)

    const rpcError = await handleIngestRequest(
      new Request("https://example.test", { method: "POST", body: JSON.stringify(payload()) }),
      { supabaseAdmin: adminMock({ rpcError: { message: "secret should not echo" } }).client },
    )
    const body = await readJson(rpcError)

    expect(rpcError.status).toBe(500)
    expect(JSON.stringify(body)).not.toContain("secret should not echo")
  })

  it("handles GET state found, missing, and missing state key", async () => {
    const found = await handleIngestRequest(new Request("https://example.test?state_key=state"), {
      supabaseAdmin: adminMock({
        stateData: {
          state_key: "state",
          state_schema_version: 2,
          state_json: { ok: true },
          dynamic_config_hash: "hash",
          version: 3,
          updated_at: "2026-03-01T00:00:00Z",
        },
      }).client,
    })
    expect(await readJson(found)).toMatchObject({ found: true, state_key: "state", version: 3 })

    const missing = await handleIngestRequest(new Request("https://example.test?state_key=state"), {
      supabaseAdmin: adminMock({ stateData: null }).client,
    })
    expect(await readJson(missing)).toEqual({ found: false })

    const bad = await handleIngestRequest(new Request("https://example.test"), {
      supabaseAdmin: adminMock().client,
    })
    expect(bad.status).toBe(400)
  })

  it("rejects unsupported methods", async () => {
    const response = await handleIngestRequest(new Request("https://example.test", { method: "PUT" }), {
      supabaseAdmin: adminMock().client,
    })
    expect(response.status).toBe(405)
  })
})
