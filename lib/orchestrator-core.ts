import 'server-only'

import { GoogleGenAI } from '@google/genai'
import type {
  OrchestratorLiveState,
  OrchestratorKeySlotState,
  OrchestratorModelHealth,
} from './types'
import {
  ORCHESTRATOR_PRIMARY_MODEL,
  ORCHESTRATOR_SECONDARY_MODEL,
  ORCHESTRATOR_TERTIARY_MODEL,
  ORCHESTRATOR_CANARY_MODEL,
  ORCHESTRATOR_WORKER_KEY_SLOTS,
  ORCHESTRATOR_RESERVED_KEY_SLOTS,
  ORCHESTRATOR_SKIPPED_KEY_SLOTS,
  ORCHESTRATOR_CHUNK_TIMEOUT_MS,
  ORCHESTRATOR_CHOKE_TIMEOUT_MS,
  ORCHESTRATOR_BURST_MAX,
  ORCHESTRATOR_BURST_STAGGER_MS,
} from './models'
import { getApiKeyN, incrementModelUsage } from './store'

export interface ChunkQueueItem {
  index: number
  status: 'pending' | 'assigned' | 'processing' | 'done' | 'failed' | 'parked'
  assignedKey?: number
  assignedModel?: string
  attempts: number
  lastError?: string
  startedAt?: number
}

export interface ChokeResult {
  ok: boolean
  latencyMs: number
  is503: boolean
  is429: boolean
  isTimeout: boolean
  reply?: string
  error?: string
}

export type ChunkExecutor = (
  chunkIndex: number,
  keySlot: number,
  apiKey: string,
  model: string,
  timeoutMs: number,
  signal: AbortSignal,
) => Promise<{ ok: boolean; is503?: boolean; is429?: boolean; isTimeout?: boolean; error?: string }>

export interface OrchestratorLogEvent {
  timestamp: number
  type: 'choke' | 'chunk' | 'failover' | 'recovery' | 'info' | 'warn' | 'error'
  model?: string
  keySlot?: number
  chunkIndex?: number
  outcome?: 'success' | '503' | '429' | 'timeout' | 'parked'
  latencyMs?: number
  message: string
}

export class ChunkScanOrchestrator {
  private activeScanId: string | null = null
  private stopping = false
  private pauseRequested = false
  private chunkQueue: ChunkQueueItem[] = []
  private keyApiMap = new Map<number, string>()
  private chokeRotationIdx = 0
  private lastBatchTime = 0
  private lastAllModelsChokeTime = 0
  private logs: OrchestratorLogEvent[] = []
  private chunkExecutor: ChunkExecutor | null = null

  private state: OrchestratorLiveState = this.createInitialState()

  constructor() {
    this.initSlots()
  }

  private createInitialState(): OrchestratorLiveState {
    const slots: Record<number, OrchestratorKeySlotState> = {}
    for (let slot = 1; slot <= 16; slot++) {
      if ((ORCHESTRATOR_RESERVED_KEY_SLOTS as readonly number[]).includes(slot)) {
        slots[slot] = {
          slot,
          status: 'reserved',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      } else if ((ORCHESTRATOR_SKIPPED_KEY_SLOTS as readonly number[]).includes(slot)) {
        slots[slot] = {
          slot,
          status: 'skipped',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      } else {
        slots[slot] = {
          slot,
          status: 'idle',
          errors503: 0,
          errors429: 0,
          timeouts: 0,
          successCount: 0,
        }
      }
    }

    const models: Record<string, OrchestratorModelHealth> = {
      [ORCHESTRATOR_PRIMARY_MODEL]: {
        modelId: ORCHESTRATOR_PRIMARY_MODEL,
        role: 'primary',
        status: 'healthy',
        effectiveParallelism: ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_PRIMARY_MODEL] || 10,
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_SECONDARY_MODEL]: {
        modelId: ORCHESTRATOR_SECONDARY_MODEL,
        role: 'secondary',
        status: 'healthy',
        effectiveParallelism: ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_SECONDARY_MODEL] || 4,
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_TERTIARY_MODEL]: {
        modelId: ORCHESTRATOR_TERTIARY_MODEL,
        role: 'tertiary',
        status: 'healthy',
        effectiveParallelism: ORCHESTRATOR_BURST_MAX[ORCHESTRATOR_TERTIARY_MODEL] || 4,
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
      [ORCHESTRATOR_CANARY_MODEL]: {
        modelId: ORCHESTRATOR_CANARY_MODEL,
        role: 'canary',
        status: 'healthy',
        effectiveParallelism: 1,
        total503: 0,
        total429: 0,
        totalTimeouts: 0,
        totalSuccess: 0,
      },
    }

    return {
      phase: 'idle',
      activeModel: ORCHESTRATOR_PRIMARY_MODEL,
      healthyModels: [ORCHESTRATOR_PRIMARY_MODEL, ORCHESTRATOR_SECONDARY_MODEL, ORCHESTRATOR_TERTIARY_MODEL],
      canaryActive: false,
      slots,
      models,
      queue: {
        pending: 0,
        assigned: 0,
        processing: 0,
        done: 0,
        failed: 0,
        retrying: 0,
      },
      velocityPerHour: 0,
      estimatedTimeRemainingSec: null,
      lastMessage: 'Orchestrator initialized and ready.',
      lastUpdated: Date.now(),
    }
  }

  private initSlots() {
    for (const slot of ORCHESTRATOR_WORKER_KEY_SLOTS) {
      const k = getApiKeyN(slot)
      if (k && k.trim()) {
        this.keyApiMap.set(slot, k.trim())
      }
    }
  }

  public registerKey(slot: number, apiKey: string) {
    if ((ORCHESTRATOR_WORKER_KEY_SLOTS as readonly number[]).includes(slot)) {
      this.keyApiMap.set(slot, apiKey.trim())
    }
  }

  public setChunkExecutor(fn: ChunkExecutor) {
    this.chunkExecutor = fn
  }

  public getState(): OrchestratorLiveState {
    this.refreshQueueCounts()
    return JSON.parse(JSON.stringify(this.state))
  }

  private onLogListeners: ((event: OrchestratorLogEvent) => void)[] = []

  public onLog(listener: (event: OrchestratorLogEvent) => void): () => void {
    this.onLogListeners.push(listener)
    return () => {
      this.onLogListeners = this.onLogListeners.filter((l) => l !== listener)
    }
  }

  public getLogs(): OrchestratorLogEvent[] {
    return this.logs.slice(-200)
  }

  public log(event: OrchestratorLogEvent) {
    this.logs.push(event)
    if (this.logs.length > 500) {
      this.logs.shift()
    }
    this.state.lastMessage = `[${event.type.toUpperCase()}] ${event.message}`
    this.state.lastUpdated = Date.now()

    for (const listener of this.onLogListeners) {
      try {
        listener(event)
      } catch {
        /* ignore callback error */
      }
    }
  }

  private refreshQueueCounts() {
    let pending = 0
    let assigned = 0
    let processing = 0
    let done = 0
    let failed = 0
    let retrying = 0

    for (const item of this.chunkQueue) {
      if (item.status === 'pending') {
        if (item.attempts > 0) retrying++
        else pending++
      } else if (item.status === 'assigned') assigned++
      else if (item.status === 'processing') processing++
      else if (item.status === 'done') done++
      else if (item.status === 'failed' || item.status === 'parked') failed++
    }

    this.state.queue = { pending, assigned, processing, done, failed, retrying }
  }

  /**
   * Load chunks into queue and prepare orchestrator.
   * Supports chunk index numbers or item objects with prior attempts and error details.
   */
  public prepareChunks(
    scanId: string,
    chunks: number[] | { index: number; attempts?: number; lastError?: string }[],
  ) {
    this.activeScanId = scanId
    this.stopping = false
    this.pauseRequested = false
    this.initSlots()

    this.chunkQueue = chunks.map((item) => {
      const idx = typeof item === 'number' ? item : item.index
      const attempts = typeof item === 'number' ? 0 : item.attempts || 0
      const lastError = typeof item === 'number' ? undefined : item.lastError
      return {
        index: idx,
        status: 'pending' as const,
        attempts,
        lastError,
      }
    })

    this.refreshQueueCounts()
    this.log({
      timestamp: Date.now(),
      type: 'info',
      message: `Loaded ${chunks.length} chunks for scan ${scanId} across ${this.keyApiMap.size} available worker keys.`,
    })
  }

  /**
   * Choke probe: tiny text request ("hy")
   * Evaluates latency: <3s = healthy, 3-15s = degrading, 15-60s = critical, >60s/503/hang = skip model.
   * Timeout 120s.
   */
  public async probeChoke(model: string, overrideKeySlot?: number): Promise<ChokeResult> {
    const availableSlots = Array.from(this.keyApiMap.keys()).filter((s) => {
      if (overrideKeySlot) return s === overrideKeySlot
      return true
    })

    if (availableSlots.length === 0) {
      return {
        ok: false,
        latencyMs: 0,
        is503: false,
        is429: false,
        isTimeout: false,
        error: `No API keys available to probe model ${model}`,
      }
    }

    this.chokeRotationIdx = (this.chokeRotationIdx + 1) % availableSlots.length
    const chosenSlot = overrideKeySlot || availableSlots[this.chokeRotationIdx]
    const apiKey = this.keyApiMap.get(chosenSlot)

    if (!apiKey) {
      return {
        ok: false,
        latencyMs: 0,
        is503: false,
        is429: false,
        isTimeout: false,
        error: `Key slot ${chosenSlot} missing API key`,
      }
    }

    const start = Date.now()
    const abortCtrl = new AbortController()
    const timeoutHandle = setTimeout(() => abortCtrl.abort(), ORCHESTRATOR_CHOKE_TIMEOUT_MS)

    try {
      const ai = new GoogleGenAI({ apiKey })
      incrementModelUsage(model, apiKey)

      const response = await ai.models.generateContent({
        model,
        contents: 'hy',
        config: {
          maxOutputTokens: 10,
          temperature: 0,
        },
      })

      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - start
      const text = (response.text || '').trim()
      // Choke = capacity probe: ANY successful response means the model is up.
      // Reply content is irrelevant (model often returns empty) — reaching here
      // without 503/429/timeout IS the success signal.
      const ok = true

      this.updateModelHealthFromLatency(model, latencyMs, 'success')
      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model,
        keySlot: chosenSlot,
        outcome: 'success',
        latencyMs,
        message: `Choke ${model} on Key ${chosenSlot} succeeded in ${(latencyMs / 1000).toFixed(2)}s (reply: "${text}").`,
      })

      return {
        ok,
        latencyMs,
        is503: false,
        is429: false,
        isTimeout: false,
        reply: text,
      }
    } catch (err: unknown) {
      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - start
      const msg = err instanceof Error ? err.message : String(err)
      const lower = msg.toLowerCase()
      const is503 = lower.includes('503') || lower.includes('high demand') || lower.includes('unavailable')
      const is429 = lower.includes('429') || lower.includes('quota') || lower.includes('resource_exhausted')
      const isTimeout = abortCtrl.signal.aborted || lower.includes('timeout') || lower.includes('aborted')

      let outcome: '503' | '429' | 'timeout' = '503'
      if (is429) outcome = '429'
      else if (isTimeout) outcome = 'timeout'

      this.updateModelHealthFromLatency(model, latencyMs, outcome)

      const slotState = this.state.slots[chosenSlot]
      if (slotState) {
        if (is503) slotState.errors503++
        else if (is429) slotState.errors429++
        else if (isTimeout) slotState.timeouts++
      }

      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model,
        keySlot: chosenSlot,
        outcome,
        latencyMs,
        message: `Choke ${model} on Key ${chosenSlot} failed with ${outcome.toUpperCase()} (${(latencyMs / 1000).toFixed(2)}s): ${msg.slice(0, 120)}`,
      })

      return {
        ok: false,
        latencyMs,
        is503,
        is429,
        isTimeout,
        error: msg,
      }
    }
  }

  private updateModelHealthFromLatency(
    model: string,
    latencyMs: number,
    outcome: 'success' | '503' | '429' | 'timeout',
  ) {
    const mh = this.state.models[model]
    if (!mh) return

    mh.lastChokeAt = Date.now()
    mh.lastChokeLatencyMs = latencyMs
    mh.lastChokeOutcome = outcome

    if (outcome === '503') {
      mh.total503++
      mh.status = 'stopped'
      mh.effectiveParallelism = 0
    } else if (outcome === '429') {
      mh.total429++
      mh.status = 'critical'
      mh.effectiveParallelism = 1
    } else if (outcome === 'timeout') {
      mh.totalTimeouts++
      mh.status = 'stopped'
      mh.effectiveParallelism = 0
    } else {
      mh.totalSuccess++
      if (latencyMs < 3000) {
        mh.status = 'healthy'
        mh.effectiveParallelism = ORCHESTRATOR_BURST_MAX[model] || 4
      } else if (latencyMs < 15000) {
        mh.status = 'degrading'
        mh.effectiveParallelism = Math.max(1, Math.floor((ORCHESTRATOR_BURST_MAX[model] || 4) / 2))
      } else {
        mh.status = 'critical'
        mh.effectiveParallelism = 1
      }
    }

    this.state.healthyModels = Object.values(this.state.models)
      .filter((m) => m.status === 'healthy' || m.status === 'degrading')
      .map((m) => m.modelId)
  }

  /**
   * Main Orchestrator Loop:
   * Choke 3.6 -> Burst 3.6 -> 503 failover to 3.7 -> probe 3.8 -> 503 all stop -> Unlimited wait on 3.5.
   */
  public async runOrchestrationLoop(): Promise<void> {
    this.stopping = false
    this.pauseRequested = false
    this.log({ timestamp: Date.now(), type: 'info', message: 'Starting chunk scan orchestrator loop.' })

    while (!this.stopping) {
      if (this.pauseRequested) {
        await new Promise((r) => setTimeout(r, 2000))
        continue
      }

      this.refreshQueueCounts()
      if (this.state.queue.pending === 0 && this.state.queue.processing === 0 && this.state.queue.assigned === 0) {
        this.state.phase = 'completed'
        this.log({
          timestamp: Date.now(),
          type: 'info',
          message: `All chunks have been scanned! Scan complete.`,
        })
        break
      }

      // Check if 15 minutes elapsed to re-choke all models
      if (Date.now() - this.lastAllModelsChokeTime > 15 * 60_000) {
        this.lastAllModelsChokeTime = Date.now()
        void this.reChokeAllModels()
      }

      // 1. PRIMARY: Probe 3.6
      this.state.phase = 'choke_primary'
      this.state.activeModel = ORCHESTRATOR_PRIMARY_MODEL
      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model: ORCHESTRATOR_PRIMARY_MODEL,
        message: `Phase [choke_primary]: Probing primary model ${ORCHESTRATOR_PRIMARY_MODEL}...`,
      })
      const primaryChoke = await this.probeChoke(ORCHESTRATOR_PRIMARY_MODEL)

      if (primaryChoke.ok) {
        this.state.phase = 'burst_primary'
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_PRIMARY_MODEL,
          message: `Phase [burst_primary]: Launching burst pipeline on ${ORCHESTRATOR_PRIMARY_MODEL} (max 10 parallel, 3s stagger)...`,
        })
        await this.executeBurst(ORCHESTRATOR_PRIMARY_MODEL)
        continue
      }

      // 2. 503 or Failover -> Immediately probe 3.7 (NO canary delay!)
      this.log({
        timestamp: Date.now(),
        type: 'failover',
        model: ORCHESTRATOR_PRIMARY_MODEL,
        message: `Primary 3.6 choked/unavailable (${primaryChoke.error || '503'}). Immediately failing over to Secondary 3.7.`,
      })

      this.state.phase = 'choke_secondary'
      this.state.activeModel = ORCHESTRATOR_SECONDARY_MODEL
      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model: ORCHESTRATOR_SECONDARY_MODEL,
        message: `Phase [choke_secondary]: Probing secondary model ${ORCHESTRATOR_SECONDARY_MODEL}...`,
      })
      const secChoke = await this.probeChoke(ORCHESTRATOR_SECONDARY_MODEL)

      if (secChoke.ok) {
        this.state.phase = 'burst_secondary'
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_SECONDARY_MODEL,
          message: `Phase [burst_secondary]: Launching burst pipeline on ${ORCHESTRATOR_SECONDARY_MODEL}...`,
        })
        // Simultaneously probe 3.8: if ready, also burst 3.8
        const tertiaryChoke = await this.probeChoke(ORCHESTRATOR_TERTIARY_MODEL)
        if (tertiaryChoke.ok) {
          await Promise.all([
            this.executeBurst(ORCHESTRATOR_SECONDARY_MODEL),
            this.executeBurst(ORCHESTRATOR_TERTIARY_MODEL),
          ])
        } else {
          await this.executeBurst(ORCHESTRATOR_SECONDARY_MODEL)
        }
        continue
      }

      // 3. Secondary 3.7 failed -> Probe Tertiary 3.8
      this.log({
        timestamp: Date.now(),
        type: 'failover',
        model: ORCHESTRATOR_SECONDARY_MODEL,
        message: `Secondary 3.7 choked/unavailable. Attempting Tertiary 3.8.`,
      })

      this.state.phase = 'choke_tertiary'
      this.state.activeModel = ORCHESTRATOR_TERTIARY_MODEL
      this.log({
        timestamp: Date.now(),
        type: 'choke',
        model: ORCHESTRATOR_TERTIARY_MODEL,
        message: `Phase [choke_tertiary]: Probing tertiary model ${ORCHESTRATOR_TERTIARY_MODEL}...`,
      })
      const tertChoke = await this.probeChoke(ORCHESTRATOR_TERTIARY_MODEL)

      if (tertChoke.ok) {
        this.state.phase = 'burst_tertiary'
        this.log({
          timestamp: Date.now(),
          type: 'info',
          model: ORCHESTRATOR_TERTIARY_MODEL,
          message: `Phase [burst_tertiary]: Launching burst pipeline on ${ORCHESTRATOR_TERTIARY_MODEL}...`,
        })
        await this.executeBurst(ORCHESTRATOR_TERTIARY_MODEL)
        continue
      }

      // 4. ALL STOP -> Unlimited Wait (Recovery Watch with 3.5 Canary)
      this.log({
        timestamp: Date.now(),
        type: 'warn',
        message: `All chunk worker models (3.6, 3.7, 3.8) are down/overloaded! Entering UNLIMITED WAIT recovery watch on 3.5 canary.`,
      })
      await this.enterUnlimitedWait()
    }
  }

  /**
   * Execute Burst on a given model:
   * - Max parallelism: 10 on 3.6, 4 on 3.7 (avoiding keys 3,10,12), 4 on 3.8.
   * - 3-second stagger between starts.
   * - Continuous pipeline: as each completes, immediately assigns next chunk without waiting for whole batch.
   * - Re-chokes before each new batch.
   */
  private async executeBurst(model: string): Promise<void> {
    const health = this.state.models[model]
    const maxParallel = health ? health.effectiveParallelism : 4

    // Respect 60s minimum interval between burst batches on same model to avoid 429 wall
    const elapsedSinceLastBatch = Date.now() - this.lastBatchTime
    if (elapsedSinceLastBatch < 60_000 && this.lastBatchTime > 0) {
      const waitMs = 60_000 - elapsedSinceLastBatch
      this.log({
        timestamp: Date.now(),
        type: 'info',
        model,
        message: `Pacing buffer: waiting ${(waitMs / 1000).toFixed(1)}s to maintain >60s burst spacing.`,
      })
      await new Promise((r) => setTimeout(r, waitMs))
    }
    this.lastBatchTime = Date.now()

    // Find available key slots that are currently idle
    const availableSlots = Array.from(this.keyApiMap.keys()).filter((s) => {
      const slotState = this.state.slots[s]
      if (!slotState || slotState.status !== 'idle') return false
      return true
    })

    const slotsToUse = availableSlots.slice(0, maxParallel)
    if (slotsToUse.length === 0) {
      await new Promise((r) => setTimeout(r, 2000))
      return
    }

    const inFlightPromises: Promise<void>[] = []

    for (let i = 0; i < slotsToUse.length; i++) {
      if (this.stopping) break
      const slot = slotsToUse[i]

      // Stagger launches by 3 seconds between slot workers
      if (i > 0) {
        await new Promise((r) => setTimeout(r, ORCHESTRATOR_BURST_STAGGER_MS))
      }

      const slotTask = (async () => {
        while (!this.stopping && !this.pauseRequested) {
          const nextChunk = this.getNextChunkForAssignment(slot, model)
          if (!nextChunk) break

          const outcome = await this.processChunkOnSlot(nextChunk, slot, model)
          if (outcome !== 'success') {
            // Model error (503 / 429 / timeout) encountered on this slot:
            // Break slot loop so the outer orchestrator can evaluate health and failover
            break
          }
        }
      })()

      inFlightPromises.push(slotTask)
    }

    if (inFlightPromises.length > 0) {
      await Promise.all(inFlightPromises)
    }
  }

  /**
   * Continuous pipeline chunk processor with strict 1-chunk-per-key locking.
   */
  private async processChunkOnSlot(
    chunk: ChunkQueueItem,
    slot: number,
    model: string,
  ): Promise<'success' | '503' | '429' | 'timeout'> {
    const slotState = this.state.slots[slot]
    const apiKey = this.keyApiMap.get(slot)
    if (!slotState || !apiKey) return '503'

    chunk.status = 'processing'
    chunk.assignedKey = slot
    chunk.assignedModel = model
    chunk.startedAt = Date.now()
    chunk.attempts++

    slotState.status = 'processing'
    slotState.activeModel = model
    slotState.activeChunk = chunk.index
    slotState.chunkStartedAt = Date.now()
    this.state.lastUpdated = Date.now()

    this.log({
      timestamp: Date.now(),
      type: 'chunk',
      model,
      keySlot: slot,
      chunkIndex: chunk.index,
      message: `Chunk #${chunk.index} assigned to Key ${slot} on ${model} (Attempt ${chunk.attempts}).`,
    })

    const abortCtrl = new AbortController()
    const timeoutHandle = setTimeout(() => abortCtrl.abort(), ORCHESTRATOR_CHUNK_TIMEOUT_MS)
    const startTime = Date.now()

    let outcome: 'success' | '503' | '429' | 'timeout' = 'success'
    let errorDetail = ''

    try {
      let result = { ok: true, is503: false, is429: false, isTimeout: false, error: '' }

      if (this.chunkExecutor) {
        result = await this.chunkExecutor(
          chunk.index,
          slot,
          apiKey,
          model,
          ORCHESTRATOR_CHUNK_TIMEOUT_MS,
          abortCtrl.signal,
        )
      } else {
        // Default simulated work / live call placeholder
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }

      clearTimeout(timeoutHandle)

      if (result.ok) {
        outcome = 'success'
        chunk.status = 'done'
        slotState.successCount++
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome: 'success',
          latencyMs: Date.now() - startTime,
          message: `Chunk #${chunk.index} completed successfully on Key ${slot} (${model}) in ${((Date.now() - startTime) / 1000).toFixed(1)}s.`,
        })
      } else {
        if (result.is429) outcome = '429'
        else if (result.isTimeout || abortCtrl.signal.aborted) outcome = 'timeout'
        else outcome = '503'
        errorDetail = result.error || 'Unknown error'
        throw new Error(errorDetail)
      }
    } catch (err: unknown) {
      clearTimeout(timeoutHandle)
      const latencyMs = Date.now() - startTime
      const msg = err instanceof Error ? err.message : String(err)
      const lower = msg.toLowerCase()

      if (lower.includes('429') || lower.includes('quota')) outcome = '429'
      else if (abortCtrl.signal.aborted || lower.includes('timeout')) outcome = 'timeout'
      else outcome = '503'

      if (outcome === '503') slotState.errors503++
      else if (outcome === '429') slotState.errors429++
      else if (outcome === 'timeout') slotState.timeouts++

      if (chunk.attempts >= 3) {
        chunk.status = 'parked'
        this.log({
          timestamp: Date.now(),
          type: 'warn',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome: 'parked',
          message: `Chunk #${chunk.index} failed 3 times — parked to continue remaining scan without blocking.`,
        })
      } else {
        // Return chunk to front of queue as pending for retry
        chunk.status = 'pending'
        this.log({
          timestamp: Date.now(),
          type: 'chunk',
          model,
          keySlot: slot,
          chunkIndex: chunk.index,
          outcome,
          latencyMs,
          message: `Chunk #${chunk.index} encountered ${outcome.toUpperCase()} on Key ${slot}. Re-queued for retry.`,
        })
      }
    } finally {
      slotState.status = 'idle'
      slotState.activeChunk = null
      slotState.chunkStartedAt = null
      slotState.lastOutcome = outcome
      slotState.lastLatencyMs = Date.now() - startTime
      this.refreshQueueCounts()
      this.state.lastUpdated = Date.now()
    }

    return outcome
  }

  private getNextChunkForAssignment(slot: number, model: string): ChunkQueueItem | null {
    // Retry queue first, then new chunks
    let target = this.chunkQueue.find((c) => c.status === 'pending' && c.attempts > 0)
    if (!target) {
      target = this.chunkQueue.find((c) => c.status === 'pending')
    }
    if (target) {
      target.status = 'assigned'
      target.assignedKey = slot
      target.assignedModel = model
      this.refreshQueueCounts()
      return target
    }
    return null
  }

  /**
   * Unlimited Wait Recovery Watch:
   * Only 3.5 canary polls (1 per minute).
   * When 3.5 recovers -> choke 3.6 (primary).
   * If 3.6 recovers -> resume primary burst 10.
   */
  private async enterUnlimitedWait(): Promise<void> {
    this.state.phase = 'recovery_watch'
    this.state.canaryActive = true
    this.state.activeModel = ORCHESTRATOR_CANARY_MODEL
    this.state.lastMessage = 'Unlimited wait active: polling 3.5 canary every 60s for capacity recovery.'
    this.state.lastUpdated = Date.now()

    while (!this.stopping) {
      if (this.pauseRequested) {
        await new Promise((r) => setTimeout(r, 2000))
        continue
      }

      this.log({
        timestamp: Date.now(),
        type: 'recovery',
        model: ORCHESTRATOR_CANARY_MODEL,
        message: `Recovery watch: polling 3.5 canary probe...`,
      })

      const canaryProbe = await this.probeChoke(ORCHESTRATOR_CANARY_MODEL)

      if (canaryProbe.ok) {
        this.log({
          timestamp: Date.now(),
          type: 'recovery',
          model: ORCHESTRATOR_CANARY_MODEL,
          message: `Canary 3.5 succeeded! Testing Primary 3.6 confirmation...`,
        })

        const primaryConfirm = await this.probeChoke(ORCHESTRATOR_PRIMARY_MODEL)
        if (primaryConfirm.ok) {
          this.log({
            timestamp: Date.now(),
            type: 'recovery',
            model: ORCHESTRATOR_PRIMARY_MODEL,
            message: `Primary 3.6 capacity restored! Resuming primary burst pipeline.`,
          })
          this.state.canaryActive = false
          return
        }
      }

      // Wait 60 seconds before next canary poll
      for (let i = 0; i < 60; i++) {
        if (this.stopping) break
        await new Promise((r) => setTimeout(r, 1000))
      }
    }
  }

  private async reChokeAllModels(): Promise<void> {
    this.log({
      timestamp: Date.now(),
      type: 'info',
      message: `15-minute periodic health check: re-probing all model tiers.`,
    })
    const models = [
      ORCHESTRATOR_PRIMARY_MODEL,
      ORCHESTRATOR_SECONDARY_MODEL,
      ORCHESTRATOR_TERTIARY_MODEL,
    ]
    for (const m of models) {
      await this.probeChoke(m)
    }
  }

  public stop() {
    this.stopping = true
    this.state.phase = 'idle'
    this.log({ timestamp: Date.now(), type: 'info', message: 'Orchestrator stopped by user.' })
  }

  public pause() {
    this.pauseRequested = true
    this.log({ timestamp: Date.now(), type: 'info', message: 'Orchestrator paused.' })
  }

  public resume() {
    this.pauseRequested = false
    this.log({ timestamp: Date.now(), type: 'info', message: 'Orchestrator resumed.' })
  }
}

// Global orchestrator singleton instance for the app
export const globalOrchestrator = new ChunkScanOrchestrator()
export { ChunkScanOrchestrator as OrchestratorCore }
