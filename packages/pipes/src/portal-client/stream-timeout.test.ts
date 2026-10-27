import { describe, expect, it } from 'vitest'

import { HttpBodyTimeoutError } from '~/http-client/index.js'

import { isBodyTimeoutError, streamBodyTimeout } from './stream-timeout.js'

function response(dataSource?: string) {
  const headers: Record<string, string> = dataSource ? { 'x-sqd-data-source': dataSource } : {}

  return new Response(null, { headers })
}

/** What Node's fetch throws when undici's own body timeout fires, as observed on Node 24. */
function undiciBodyTimeout() {
  const cause = Object.assign(new Error('Body Timeout Error'), { code: 'UND_ERR_BODY_TIMEOUT' })

  return new TypeError('terminated', { cause })
}

describe('streamBodyTimeout', () => {
  it('gives a real-time response 30 s', () => {
    expect(streamBodyTimeout(response('real_time'))).toBe(30_000)
  })

  // An archive response may wait out one worker timeout and a second worker.
  it('gives an archive response 150 s', () => {
    expect(streamBodyTimeout(response('network'))).toBe(150_000)
  })

  it('gives a response that names no source the archive timeout', () => {
    expect(streamBodyTimeout(response())).toBe(150_000)
  })
})

describe('isBodyTimeoutError', () => {
  it('matches the timeout of the HTTP client', () => {
    expect(isBodyTimeoutError(new HttpBodyTimeoutError(100))).toBe(true)
  })

  it("matches undici's body timeout behind fetch's `terminated`", () => {
    expect(isBodyTimeoutError(undiciBodyTimeout())).toBe(true)
  })

  // A reset is re-requested too, but as a dropped connection, not as a stall to report.
  it('does not match a reset connection', () => {
    const reset = new TypeError('terminated', { cause: Object.assign(new Error('read'), { code: 'ECONNRESET' }) })

    expect(isBodyTimeoutError(reset)).toBe(false)
  })
})
