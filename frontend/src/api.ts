export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function searchParams(
  values: Record<
    string,
    string | number | boolean | null | undefined | string[]
  >,
): string {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    if (Array.isArray(value)) value.forEach((item) => params.append(key, item));
    else params.set(key, String(value));
  });
  const query = params.toString();
  return query ? `?${query}` : "";
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.body && !(init.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, { ...init, headers });
  if (!response.ok) {
    let message = `La requête a échoué (${response.status})`;
    try {
      const payload = (await response.json()) as { detail?: unknown };
      if (typeof payload.detail === "string" && payload.detail) {
        message = payload.detail;
      } else if (Array.isArray(payload.detail)) {
        const messages = payload.detail.flatMap((item: unknown) => {
          if (!item || typeof item !== "object" || !("msg" in item)) return [];
          const error = item as { msg?: unknown; loc?: unknown };
          if (typeof error.msg !== "string") return [];
          const location = Array.isArray(error.loc)
            ? error.loc.filter((part) => part !== "body").join(".")
            : "";
          return [location ? `${location}: ${error.msg}` : error.msg];
        });
        if (messages.length) message = messages.join("; ");
      }
    } catch {
      // Keep the fallback message for non-JSON errors.
    }
    throw new ApiError(response.status, message);
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export async function download(
  path: string,
  fallbackName: string,
): Promise<void> {
  const response = await fetch(path);
  if (!response.ok) throw new ApiError(response.status, "L’export a échoué");
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition");
  const match = disposition?.match(/filename="?([^";]+)"?/);
  const anchor = document.createElement("a");
  anchor.href = URL.createObjectURL(blob);
  anchor.download = match?.[1] ?? fallbackName;
  anchor.click();
  URL.revokeObjectURL(anchor.href);
}
