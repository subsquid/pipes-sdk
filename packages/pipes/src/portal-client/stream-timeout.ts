import { HttpBodyTimeoutError } from '~/http-client/index.js'

const REAL_TIME_BODY_TIMEOUT_MS = 30_000
const ARCHIVE_BODY_TIMEOUT_MS = 150_000

/**
 * How long a stream response may send no bytes before the client cuts it and requests the rest.
 * It must outlast the longest pause of a healthy response, or every such pause becomes a reconnect.
 *
 * Bytes, not blocks: a block that takes longer than the timeout to download would never arrive,
 * because each new request starts it again from its first byte.
 */
export function streamBodyTimeout(res: Response): number {
  // The portal ends a real-time response itself once its source is silent for 20 s.
  if (res.headers.get('x-sqd-data-source') === 'real_time') {
    return REAL_TIME_BODY_TIMEOUT_MS
  }

  // An archive response can wait for a worker to time out at 60 s, then for a second worker.
  // A portal that does not name its source gets the same, longer timeout.
  return ARCHIVE_BODY_TIMEOUT_MS
}

/** Our own body timeout, or undici's: fetch reports that one as `TypeError: terminated` with it as the cause. */
export function isBodyTimeoutError(err: unknown): boolean {
  if (err instanceof HttpBodyTimeoutError) {
    return true
  }
  if (!(err instanceof Error)) {
    return false
  }

  const code = (err.cause as any)?.code ?? (err as any).code

  return code === 'UND_ERR_BODY_TIMEOUT'
}
