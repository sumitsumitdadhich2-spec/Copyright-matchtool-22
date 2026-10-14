#!/usr/bin/env node
/**
 * Shiva MatchTool — Chunk Scan Orchestrator (CLI & Background Runner)
 * ====================================================================
 * Implements the measured 2026-10-14 testing findings:
 *   - 16 Key slots (1-13, 15, 16). Key 1 reserved. Slot 14 skipped.
 *   - Primary: gemini-3.6-flash (burst 10, continuous pipeline)
 *   - Secondary: gemini-3.7-flash (burst 4, avoids key 3, 10, 12)
 *   - Tertiary: gemini-3.8-flash (burst 4)
 *   - Canary: gemini-3.5-flash (probes 1/min during all-stop recovery watch)
 *   - Choke timeout 120s, Chunk timeout 600s
 *   - Collision-free chunk queue with retry prioritizing
 *   - Live terminal matrix and structured file logging
 */

import fs from 'node:fs'
import path from 'node:path'

const ROOT = process.cwd()
const DEFAULT_BASE_URL = process.env.MATCHTOOL_URL || 'http://localhost:3000'
const LOG_FILE = path.join(ROOT, 'data', 'orchestrator-run.log')

// ---------- Configuration ----------
const PRIMARY_MODEL = 'gemini-3.6-flash'
const SECONDARY_MODEL = 'gemini-3.7-flash'
const TERTIARY_MODEL = 'gemini-3.8-flash'
const CANARY_MODEL = 'gemini-3.5-flash'

const WORKER_KEY_SLOTS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16]
const AVOID_37_SLOTS = [3, 10, 12]

const CHOKE_TIMEOUT_MS = 120_000 // 120s for tiny probes
const CHUNK_TIMEOUT_MS = 600_000 // 600s (10 min) for chunk video requests
const BURST_STAGGER_MS = 3_000

// ---------- Parse CLI arguments ----------
const args = process.argv.slice(2)
function getArg(name, defVal) {
  const i = args.indexOf(`--${name}`)
  return i >= 0 && i + 1 < args.length ? args[i + 1] : defVal
}

const BASE_URL = getArg('url', DEFAULT_BASE_URL)
const USERNAME = getArg('user', 'admin')
const PASSWORD = getArg('pass', 'admin')
const CHUNKS_DIR = getArg('chunks-dir', path.join(ROOT, 'data', 'chunks'))
const CHUNKS_COUNT = Number(getArg('chunks', '120'))
const FPS = Number(getArg('fps', '1'))
const TIMELAPSE = Number(getArg('timelapse', '1'))

let authCookie = null
let authExpiresAt = 0

// ---------- Logger ----------
function log(type, msg, meta = {}) {
  const ts = new Date().toISOString()
  const line = `[${ts}] [${type.toUpperCase().padEnd(8)}] ${msg} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}\n`
  process.stdout.write(line)
  try {
    fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true })
    fs.appendFileSync(LOG_FILE, line)
  } catch {}
}

// ---------- Authentication ----------
async function ensureAuth() {
  if (authCookie && Date.now() < authExpiresAt) return authCookie

  log('AUTH', `Authenticating with ${BASE_URL} as ${USERNAME}...`)
  const res = await fetch(`${BASE_URL}/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: USERNAME, password: PASSWORD }),
  })

  if (!res.ok) {
    throw new Error(`Authentication failed: HTTP ${res.status} ${res.statusText}`)
  }

  const setCookie = res.headers.get('set-cookie')
  if (setCookie) {
    authCookie = setCookie.split(';')[0]
    authExpiresAt = Date.now() + 50 * 60_000 // 50 minutes valid
    log('AUTH', 'Authentication successful, session token secured.')
    return authCookie
  }
  throw new Error('No cookie received from login endpoint.')
}

// ---------- Choke Probe ----------
async function probeChoke(model, keySlot) {
  const cookie = await ensureAuth()
  const start = Date.now()
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), CHOKE_TIMEOUT_MS)

  try {
    const res = await fetch(`${BASE_URL}/api/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Cookie: cookie,
      },
      body: JSON.stringify({
        prompt: 'Reply with exactly: OK',
        model,
        keySelection: 'slot',
        keySlot,
      }),
      signal: controller.signal,
    })

    clearTimeout(timeoutId)
    const latencyMs = Date.now() - start
    const data = await res.json().catch(() => ({}))

    if (res.ok) {
      const reply = (data.reply || '').trim()
      const ok = reply.toUpperCase().includes('OK')
      log('CHOKE', `Choke ${model} (Slot ${keySlot}) SUCCESS in ${(latencyMs / 1000).toFixed(2)}s: "${reply}"`, {
        latencyMs,
        model,
        keySlot,
        outcome: 'success',
      })
      return { ok, latencyMs, outcome: 'success', data }
    }

    const errStr = data.error || ''
    const is503 = Boolean(data.is503) || errStr.includes('503') || errStr.includes('high demand')
    const is429 = Boolean(data.is429) || errStr.includes('429') || errStr.includes('quota')
    const outcome = is503 ? '503' : is429 ? '429' : 'error'

    log('CHOKE', `Choke ${model} (Slot ${keySlot}) FAILED (${outcome}) in ${(latencyMs / 1000).toFixed(2)}s: ${errStr.slice(0, 100)}`, {
      latencyMs,
      model,
      keySlot,
      outcome,
    })

    return { ok: false, latencyMs, outcome, is503, is429, error: errStr }
  } catch (err) {
    clearTimeout(timeoutId)
    const latencyMs = Date.now() - start
    const isTimeout = controller.signal.aborted
    const outcome = isTimeout ? 'timeout' : 'error'
    log('CHOKE', `Choke ${model} (Slot ${keySlot}) ${outcome.toUpperCase()} after ${(latencyMs / 1000).toFixed(2)}s`, {
      latencyMs,
      model,
      keySlot,
      outcome,
    })
    return { ok: false, latencyMs, outcome, isTimeout, error: String(err) }
  }
}

// ---------- Chunk Video Request ----------
async function sendChunkRequest(chunkIndex, model, keySlot, videoFilePath) {
  const cookie = await ensureAuth()
  const start = Date.now()
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), CHUNK_TIMEOUT_MS)

  try {
    let form
    if (typeof globalThis.FormData !== 'undefined' && fs.existsSync(videoFilePath)) {
      const fileBuffer = fs.readFileSync(videoFilePath)
      const blob = new Blob([fileBuffer], { type: 'video/mp4' })
      form = new globalThis.FormData()
      form.append('prompt', `Scan chunk #${chunkIndex}: identify visual match timestamps.`)
      form.append('model', model)
      form.append('keySelection', 'slot')
      form.append('keySlot', String(keySlot))
      form.append('fps', String(FPS))
      form.append('timelapse', String(TIMELAPSE))
      form.append('video', blob, path.basename(videoFilePath))
    }

    const res = await fetch(`${BASE_URL}/api/chat`, {
      method: 'POST',
      headers: {
        Cookie: cookie,
        ...(form ? {} : { 'Content-Type': 'application/json' }),
      },
      body: form || JSON.stringify({
        prompt: `Scan chunk #${chunkIndex}: identify visual match timestamps.`,
        model,
        keySelection: 'slot',
        keySlot,
        fps: FPS,
        timelapse: TIMELAPSE,
      }),
      signal: controller.signal,
    })

    clearTimeout(timeoutId)
    const latencyMs = Date.now() - start
    const data = await res.json().catch(() => ({}))

    if (res.ok) {
      log('CHUNK', `Chunk #${chunkIndex} SUCCESS on Key ${keySlot} (${model}) in ${(latencyMs / 1000).toFixed(1)}s`, {
        chunkIndex,
        keySlot,
        model,
        latencyMs,
        outcome: 'success',
      })
      return { ok: true, latencyMs, outcome: 'success', data }
    }

    const errStr = data.error || ''
    const is503 = Boolean(data.is503) || errStr.includes('503') || errStr.includes('high demand')
    const is429 = Boolean(data.is429) || errStr.includes('429') || errStr.includes('quota')
    const outcome = is503 ? '503' : is429 ? '429' : 'error'

    log('CHUNK', `Chunk #${chunkIndex} FAILED (${outcome}) on Key ${keySlot} (${model}): ${errStr.slice(0, 100)}`, {
      chunkIndex,
      keySlot,
      model,
      latencyMs,
      outcome,
    })

    return { ok: false, latencyMs, outcome, is503, is429, error: errStr }
  } catch (err) {
    clearTimeout(timeoutId)
    const latencyMs = Date.now() - start
    const isTimeout = controller.signal.aborted
    const outcome = isTimeout ? 'timeout' : 'error'
    log('CHUNK', `Chunk #${chunkIndex} ${outcome.toUpperCase()} after ${(latencyMs / 1000).toFixed(1)}s on Key ${keySlot}`, {
      chunkIndex,
      keySlot,
      model,
      latencyMs,
      outcome,
    })
    return { ok: false, latencyMs, outcome, isTimeout, error: String(err) }
  }
}

// ---------- Main Orchestrator Runner ----------
async function main() {
  log('INIT', '=======================================================')
  log('INIT', 'Shiva MatchTool Chunk Scan Orchestrator Starting...')
  log('INIT', `Target URL: ${BASE_URL}`)
  log('INIT', `Total Chunks: ${CHUNKS_COUNT}`)
  log('INIT', `Available Worker Slots: ${WORKER_KEY_SLOTS.join(', ')} (Key 1 reserved, Slot 14 skipped)`)
  log('INIT', '=======================================================')

  // Initialize central queue
  const queue = Array.from({ length: CHUNKS_COUNT }, (_, i) => ({
    index: i,
    status: 'pending',
    attempts: 0,
    assignedKey: null,
    assignedModel: null,
  }))

  let chokeKeyRotation = 0
  function getChokeSlot(model) {
    const slots = WORKER_KEY_SLOTS.filter((s) => {
      if (model === SECONDARY_MODEL) return !AVOID_37_SLOTS.includes(s)
      return true
    })
    chokeKeyRotation = (chokeKeyRotation + 1) % slots.length
    return slots[chokeKeyRotation]
  }

  // Active slot tracking (strict 1-chunk-per-key)
  const slotBusy = new Map()
  for (const s of WORKER_KEY_SLOTS) slotBusy.set(s, false)

  let completedChunks = 0
  const startTime = Date.now()

  while (completedChunks < CHUNKS_COUNT) {
    const pendingList = queue.filter((c) => c.status === 'pending')
    if (pendingList.length === 0 && !Array.from(slotBusy.values()).some(Boolean)) {
      break
    }

    // 1. PRIMARY: Probe 3.6
    log('ORCH', `Probing Primary Model (${PRIMARY_MODEL})...`)
    const primaryChoke = await probeChoke(PRIMARY_MODEL, getChokeSlot(PRIMARY_MODEL))

    if (primaryChoke.ok) {
      log('ORCH', `Primary 3.6 is HEALTHY. Launching burst (up to 10 parallel)...`)
      await launchBurst(PRIMARY_MODEL, 10)
      continue
    }

    // 2. 503 -> Immediately Probe 3.7
    log('ORCH', `Primary 3.6 choked (${primaryChoke.outcome}). Immediate failover to Secondary (${SECONDARY_MODEL})...`)
    const secChoke = await probeChoke(SECONDARY_MODEL, getChokeSlot(SECONDARY_MODEL))

    if (secChoke.ok) {
      log('ORCH', `Secondary 3.7 is HEALTHY. Checking Tertiary 3.8 in parallel...`)
      const tertChoke = await probeChoke(TERTIARY_MODEL, getChokeSlot(TERTIARY_MODEL))
      if (tertChoke.ok) {
        log('ORCH', `Both 3.7 and 3.8 are ready. Launching combined burst (4+4 = 8 parallel)...`)
        await Promise.all([launchBurst(SECONDARY_MODEL, 4), launchBurst(TERTIARY_MODEL, 4)])
      } else {
        await launchBurst(SECONDARY_MODEL, 4)
      }
      continue
    }

    // 3. 503 -> Probe 3.8
    log('ORCH', `Secondary 3.7 choked (${secChoke.outcome}). Attempting Tertiary (${TERTIARY_MODEL})...`)
    const tertChoke = await probeChoke(TERTIARY_MODEL, getChokeSlot(TERTIARY_MODEL))

    if (tertChoke.ok) {
      log('ORCH', `Tertiary 3.8 is HEALTHY. Launching burst (up to 4 parallel)...`)
      await launchBurst(TERTIARY_MODEL, 4)
      continue
    }

    // 4. ALL STOP -> Unlimited Wait on 3.5 Canary
    log('ORCH', `ALL MODELS ARE DOWN (503 / high demand)! Entering UNLIMITED WAIT recovery watch on 3.5 canary...`)
    await unlimitedWaitRecovery()
  }

  const durationMin = ((Date.now() - startTime) / 60_000).toFixed(1)
  log('DONE', `Scanning completed! ${completedChunks}/${CHUNKS_COUNT} chunks processed in ${durationMin} minutes.`)

  async function launchBurst(model, maxParallel) {
    const freeSlots = WORKER_KEY_SLOTS.filter((s) => {
      if (slotBusy.get(s)) return false
      if (model === SECONDARY_MODEL && AVOID_37_SLOTS.includes(s)) return false
      return true
    }).slice(0, maxParallel)

    if (freeSlots.length === 0) {
      await new Promise((r) => setTimeout(r, 2000))
      return
    }

    const tasks = []
    for (const slot of freeSlots) {
      // Pick next chunk: retry first, then new
      let chunk = queue.find((c) => c.status === 'pending' && c.attempts > 0)
      if (!chunk) chunk = queue.find((c) => c.status === 'pending')
      if (!chunk) break

      chunk.status = 'assigned'
      chunk.assignedKey = slot
      chunk.assignedModel = model
      chunk.attempts++
      slotBusy.set(slot, true)

      // Stagger start by 3 seconds
      await new Promise((r) => setTimeout(r, BURST_STAGGER_MS))

      const p = (async () => {
        const dummyPath = path.join(CHUNKS_DIR, `chunk-${String(chunk.index).padStart(4, '0')}.mp4`)
        const res = await sendChunkRequest(chunk.index, model, slot, dummyPath)
        slotBusy.set(slot, false)

        if (res.ok) {
          chunk.status = 'done'
          completedChunks++
        } else {
          if (chunk.attempts >= 3) {
            chunk.status = 'parked'
            log('WARN', `Chunk #${chunk.index} parked after 3 failures. Continuing remaining scan.`)
          } else {
            chunk.status = 'pending'
            log('RETRY', `Chunk #${chunk.index} returned to queue for retry (Attempt ${chunk.attempts}/3).`)
          }
        }
      })()

      tasks.push(p)
    }

    if (tasks.length > 0) {
      await Promise.all(tasks)
    }
  }

  async function unlimitedWaitRecovery() {
    while (true) {
      log('CANARY', `Recovery Watch: probing 3.5 canary...`)
      const canaryResult = await probeChoke(CANARY_MODEL, getChokeSlot(CANARY_MODEL))
      if (canaryResult.ok) {
        log('CANARY', `3.5 Canary succeeded! Immediately confirming Primary 3.6...`)
        const primaryConfirm = await probeChoke(PRIMARY_MODEL, getChokeSlot(PRIMARY_MODEL))
        if (primaryConfirm.ok) {
          log('CANARY', `Primary 3.6 capacity RESTORED! Resuming primary execution.`)
          return
        }
      }
      log('CANARY', `Models still recovering. Waiting 60s before next canary check...`)
      await new Promise((r) => setTimeout(r, 60_000))
    }
  }
}

main().catch((err) => {
  log('FATAL', `Orchestrator encountered fatal error: ${err.stack || err}`)
  process.exit(1)
})
