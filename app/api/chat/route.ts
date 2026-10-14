import { NextRequest, NextResponse } from 'next/server'
import { GoogleGenAI } from '@google/genai'
import { getSession } from '@/lib/users'
import { getChatKeyN, getAllChatApiKeys } from '@/lib/chat-keys'
import { incrementModelUsage } from '@/lib/store'
import { runFfmpeg } from '@/lib/ffmpeg-pool'
import fs from 'node:fs'
import path from 'node:path'
import os from 'node:os'

export const runtime = 'nodejs'

const ALLOWED_MODELS = [
  'gemini-3.8-flash',
  'gemini-3.7-flash',
  'gemini-3.6-flash',
  'gemini-3.5-flash',
  'gemini-3.5-flash-lite',
  'gemini-3-flash-preview',
  'gemini-3.1-pro-preview',
  'gemini-3.1-flash-lite',
  'gemini-2.5-flash',
  'gemini-2.5-pro',
]

interface ChatHistoryItem {
  role: 'user' | 'model'
  content: string
}

export async function POST(req: NextRequest) {
  const session = await getSession()
  if (!session) {
    return NextResponse.json({ error: 'Please log in to use AI Chat.' }, { status: 401 })
  }

  let tempInputPath: string | null = null
  let tempProcessedPath: string | null = null
  let uploadedGeminiFileName: string | null = null
  let aiClient: GoogleGenAI | null = null
  let model = 'gemini-3.8-flash'
  let keySlot: number | null = null
  let keySelection = 'auto'
  let customKey = ''
  let resolvedApiKey = ''
  let requestSentAt: number | null = null

  try {
    const contentType = req.headers.get('content-type') || ''
    let prompt = ''
    let fps = 1
    let timelapse = 1
    let history: ChatHistoryItem[] = []
    let videoFile: File | null = null

    if (contentType.includes('multipart/form-data')) {
      const formData = await req.formData()
      prompt = (formData.get('prompt') as string) || ''
      model = (formData.get('model') as string) || 'gemini-3.8-flash'
      keySelection = (formData.get('keySelection') as string) || 'auto'
      const slotStr = formData.get('keySlot') as string
      if (slotStr) keySlot = Number.parseInt(slotStr, 10) || null
      customKey = ((formData.get('customKey') as string) || '').trim()
      fps = Math.max(0.1, Math.min(60, Number.parseFloat((formData.get('fps') as string) || '1') || 1))
      timelapse = Math.max(1, Math.min(120, Number.parseFloat((formData.get('timelapse') as string) || '1') || 1))

      const historyRaw = formData.get('history') as string
      if (historyRaw) {
        try {
          history = JSON.parse(historyRaw)
        } catch {
          history = []
        }
      }

      const fileEntry = formData.get('video')
      if (fileEntry instanceof File && fileEntry.size > 0) {
        videoFile = fileEntry
      }
    } else {
      const body = await req.json()
      prompt = body.prompt || ''
      model = body.model || 'gemini-3.8-flash'
      keySelection = body.keySelection || 'auto'
      keySlot = typeof body.keySlot === 'number' ? body.keySlot : null
      customKey = (body.customKey || '').trim()
      fps = Math.max(0.1, Math.min(60, Number(body.fps) || 1))
      timelapse = Math.max(1, Math.min(120, Number(body.timelapse) || 1))
      history = Array.isArray(body.history) ? body.history : []
    }

    if (!ALLOWED_MODELS.includes(model)) {
      model = 'gemini-3.8-flash'
    }

    // Resolve API key from isolated chat keys store (no usage or exhaustion records tracked)
    resolvedApiKey = ''
    if (customKey) {
      resolvedApiKey = customKey
    } else if (keySelection === 'slot' && keySlot && keySlot >= 1 && keySlot <= 20) {
      const k = await getChatKeyN(session.username, keySlot)
      if (k) resolvedApiKey = k
    } else {
      // Auto: Try user chat keys in order (auto-copied from settings without counting usage)
      const chatKeys = await getAllChatApiKeys(session.username)
      if (chatKeys.length > 0) {
        resolvedApiKey = chatKeys[0]
      } else if (process.env.GEMINI_API_KEY) {
        resolvedApiKey = process.env.GEMINI_API_KEY
      }
    }

    if (!resolvedApiKey || !resolvedApiKey.trim()) {
      return NextResponse.json(
        {
          error:
            'No Gemini API Key found in Chat Keys. Keys in Settings are auto-copied, or you can enter a custom key.',
        },
        { status: 400 },
      )
    }

    aiClient = new GoogleGenAI({
      apiKey: resolvedApiKey.trim(),
      httpOptions: {
        headers: {
          'User-Agent': 'aistudio-build',
        },
      },
    })

    // Handle video processing if video attached
    let geminiFileUri: string | null = null

    if (videoFile) {
      const ext = path.extname(videoFile.name) || '.mp4'
      const randId = Math.random().toString(36).substring(2, 9)
      tempInputPath = path.join(os.tmpdir(), `cmt-chat-in-${Date.now()}-${randId}${ext}`)

      const buffer = Buffer.from(await videoFile.arrayBuffer())
      await fs.promises.writeFile(tempInputPath, buffer)

      let uploadFilePath = tempInputPath

      // Only perform FFmpeg processing if timelapse speedup factor > 1 is explicitly requested.
      // Setting 24 fps (or any fps) does NOT re-encode the video — Gemini natively samples
      // frames via videoMetadata: { fps } directly on Google's servers without CPU re-encode lag!
      if (timelapse > 1) {
        tempProcessedPath = path.join(os.tmpdir(), `cmt-chat-out-${Date.now()}-${randId}.mp4`)
        const ptsFactor = (1 / timelapse).toFixed(4)
        try {
          await runFfmpeg(
            [
              '-y',
              '-i',
              tempInputPath,
              '-vf',
              `setpts=${ptsFactor}*PTS`,
              '-c:v',
              'libx264',
              '-preset',
              'ultrafast',
              '-crf',
              '28',
              '-an',
              tempProcessedPath,
            ],
            { label: 'chat-timelapse', owner: 'chat' },
          )
          uploadFilePath = tempProcessedPath
        } catch (ffErr) {
          console.warn('[AI Chat] Timelapse speedup failed, falling back to direct video upload:', ffErr)
          uploadFilePath = tempInputPath
        }
      }

      // Upload directly to Gemini Files API
      try {
        const uploadResult = await aiClient.files.upload({
          file: uploadFilePath,
          mimeType: 'video/mp4',
        })

        uploadedGeminiFileName = uploadResult.name || null
        let f = uploadResult

        // Wait until active (poll)
        const deadline = Date.now() + 3 * 60_000
        while (f.state === 'PROCESSING') {
          if (Date.now() > deadline) {
            throw new Error('Google video processing timeout (exceeded 3 minutes)')
          }
          await new Promise((resolve) => setTimeout(resolve, 2000))
          f = await aiClient.files.get({ name: f.name! })
        }

        if (f.state !== 'ACTIVE') {
          throw new Error(`Google video state is '${f.state}', unable to analyze.`)
        }

        geminiFileUri = f.uri || null
      } catch (uploadErr) {
        console.error('[AI Chat] Gemini file upload error:', uploadErr)
        throw new Error(
          `Google Files API upload failed: ${
            uploadErr instanceof Error ? uploadErr.message : String(uploadErr)
          }`,
        )
      }
    }

    // Build contents array with sanitized conversation history (alternating user/model)
    const contents: Array<{
      role: 'user' | 'model'
      parts: Array<{
        text?: string
        fileData?: { fileUri: string; mimeType: string }
        videoMetadata?: { fps?: number }
      }>
    }> = []

    if (Array.isArray(history) && history.length > 0) {
      const sanitizedHistory: Array<{ role: 'user' | 'model'; text: string }> = []
      for (const item of history) {
        if (item && item.content && (item.role === 'user' || item.role === 'model')) {
          sanitizedHistory.push({ role: item.role, text: item.content.trim() })
        }
      }

      let expectedRole: 'user' | 'model' = 'user'
      for (const h of sanitizedHistory) {
        if (h.role === expectedRole) {
          contents.push({
            role: h.role,
            parts: [{ text: h.text }],
          })
          expectedRole = expectedRole === 'user' ? 'model' : 'user'
        } else if (h.role === 'user' && expectedRole === 'model') {
          const last = contents[contents.length - 1]
          if (last && last.role === 'user' && last.parts[0]?.text) {
            last.parts[0].text += `\n\n${h.text}`
          }
        }
      }
      // If the last history turn is user, drop it so current new user turn doesn't double-up
      if (contents.length > 0 && contents[contents.length - 1].role === 'user') {
        contents.pop()
      }
    }

    const currentParts: Array<{
      text?: string
      fileData?: { fileUri: string; mimeType: string }
      videoMetadata?: { fps?: number }
    }> = []

    if (geminiFileUri) {
      currentParts.push({
        fileData: {
          fileUri: geminiFileUri,
          mimeType: 'video/mp4',
        },
        videoMetadata: {
          fps: fps, // Natively instructs Gemini to sample frames at user-requested FPS (e.g. 24 fps) without CPU re-encode!
        },
      })
    }

    const userPrompt = prompt.trim() || (videoFile ? 'Analyze this video clip and detail key actions, scene changes, and timestamps.' : 'Hello!')
    currentParts.push({ text: userPrompt })

    contents.push({
      role: 'user',
      parts: currentParts,
    })

    requestSentAt = Date.now()
    // User instruction: Every request made in AI chat must count toward settings quota for that key and model!
    const usedToday = incrementModelUsage(model, resolvedApiKey)

    const response = await aiClient.models.generateContent({
      model,
      contents,
      config: {
        systemInstruction:
          'You are an intelligent, articulate, and accurate AI assistant. When analyzing videos or clips, break down visual evidence chronologically, state exact timestamps (MM:SS), mention visual actions, objects, lighting, camera angles, and text. Use clear markdown formatting, bullet points, and code formatting where suitable.',
      },
    })

    const requestRespondedAt = Date.now()
    const responseText = response.text || ''

    return NextResponse.json({
      reply: responseText,
      model,
      keySlot: keySlot || 1,
      keyUsed: keySlot ? `Key ${keySlot}` : 'Key 1',
      usedToday,
      sentAt: requestSentAt,
      respondedAt: requestRespondedAt,
      durationMs: requestRespondedAt - requestSentAt,
      fps: videoFile ? fps : undefined,
      timelapse: videoFile ? timelapse : undefined,
      hadVideo: Boolean(videoFile),
      timestamp: Date.now(),
    })
  } catch (err: unknown) {
    console.error('[AI Chat] Request failed:', err)
    const errorMsg = err instanceof Error ? err.message : String(err)
    const errObj = err && typeof err === 'object' ? (err as Record<string, unknown>) : null
    const errStatus = (errObj?.status ?? (errObj?.error as Record<string, unknown>)?.code ?? (errObj?.error as Record<string, unknown>)?.status)
    const lower = `${errorMsg} ${String(errStatus || '')}`.toLowerCase()
    const is503 = lower.includes('503') || lower.includes('high demand') || lower.includes('unavailable')
    const is429 = lower.includes('429') || lower.includes('quota') || lower.includes('rate limit') || lower.includes('resource_exhausted')
    const now = Date.now()
    const finalSentAt = requestSentAt ?? now
    return NextResponse.json(
      {
        error: errorMsg,
        is503,
        is429,
        model: model || 'gemini-3.8-flash',
        keySlot: keySlot || 1,
        keyUsed: keySlot ? `Key ${keySlot}` : 'Key 1',
        sentAt: finalSentAt,
        respondedAt: now,
        durationMs: Math.max(0, now - finalSentAt),
      },
      { status: 400 },
    )
  } finally {
    // Cleanup local temporary files
    if (tempInputPath && fs.existsSync(tempInputPath)) {
      try {
        await fs.promises.unlink(tempInputPath)
      } catch {}
    }
    if (tempProcessedPath && fs.existsSync(tempProcessedPath)) {
      try {
        await fs.promises.unlink(tempProcessedPath)
      } catch {}
    }
    // Delete file from Google storage to keep user's quota clean
    if (aiClient && uploadedGeminiFileName) {
      try {
        await aiClient.files.delete({ name: uploadedGeminiFileName })
      } catch {}
    }
  }
}
