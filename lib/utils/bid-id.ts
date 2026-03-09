export const MAX_BID_ID_LENGTH = 128;
export const MIN_BID_ID_LENGTH = 3;

export function isValidBidId(value: string) {
  const trimmed = value.trim();

  if (
    trimmed.length < MIN_BID_ID_LENGTH ||
    trimmed.length > MAX_BID_ID_LENGTH
  ) {
    return false;
  }

  for (const character of trimmed) {
    const code = character.charCodeAt(0);
    const isControlCharacter = code < 33 || code === 127;
    const isCsvDelimiter =
      character === "," ||
      character === '"' ||
      character === "'" ||
      character === ";" ||
      character === "`";

    if (isControlCharacter || isCsvDelimiter) {
      return false;
    }
  }

  return true;
}
