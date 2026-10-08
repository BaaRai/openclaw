import { hasActiveDeliveryOwner, type QueuedDelivery } from "./delivery-queue-types.js";

const DEFAULT_MAX_RETRIES = 5;

const PERMANENT_ERROR_PATTERNS: readonly RegExp[] = [
  /no conversation reference found/i,
  /chat not found/i,
  /user not found/i,
  /bot.*not.*member/i,
  /bot was blocked by the user/i,
  /forbidden: bot was kicked/i,
  /chat_id is empty/i,
  /recipient is not a valid/i,
  /ambiguous .* recipient/i,
  /User .* not in room/i,
];

export function resolveMaxRetries(entry: QueuedDelivery): number {
  const configured = entry.maxRetries;
  return typeof configured === "number" && Number.isInteger(configured) && configured > 0
    ? configured
    : DEFAULT_MAX_RETRIES;
}

export function isPermanentDeliveryError(error: string): boolean {
  return PERMANENT_ERROR_PATTERNS.some((re) => re.test(error));
}

/** Select before claiming locally; authoritative reload still fences later ownership changes. */
export function selectDeliveryRecoveryCandidates(
  entries: readonly QueuedDelivery[],
  matches?: (entry: QueuedDelivery, now: number) => boolean,
): QueuedDelivery[] {
  const now = Date.now();
  return entries.filter(
    (entry) =>
      entry.settlement || (!hasActiveDeliveryOwner(entry, now) && (matches?.(entry, now) ?? true)),
  );
}
