/**
 * Validates user input for the RPC endpoint prompt.
 * Returns `true` when the value is acceptable, or an error message string otherwise.
 * Blank is allowed — it becomes an empty RPC_URL= placeholder in the generated .env.
 */
export function validateRpcUrl(value: string): true | string {
  const trimmed = value.trim()
  if (trimmed === '') {
    return true
  }

  let url: URL
  try {
    url = new URL(trimmed)
  } catch {
    return 'Enter a valid URL (e.g. https://eth-mainnet.example.com/v2/YOUR_KEY) or press Enter to skip'
  }

  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    return 'The RPC URL must use http:// or https://'
  }

  return true
}
