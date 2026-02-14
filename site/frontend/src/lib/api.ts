export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://127.0.0.1:8787';

export type GlobalSearchResponse = {
  providers: any[];
  hcpcs: any[];
};

async function fetchJson<T>(url: string | URL): Promise<T> {
  let r: Response;
  try {
    r = await fetch(url.toString());
  } catch (e: any) {
    const msg = [
      `Failed to reach API at ${API_BASE_URL}.`,
      `Start the backend with: (cd site/backend && cargo run --release -- serve --host 127.0.0.1 --port 8787)`,
      `Or set VITE_API_BASE_URL (e.g. http://127.0.0.1:8787) in site/frontend/.env.`,
      e?.message ? `Original error: ${e.message}` : undefined
    ]
      .filter(Boolean)
      .join(' ');
    throw new Error(msg);
  }

  if (!r.ok) {
    const body = await r.text().catch(() => '');
    throw new Error(`API error ${r.status} ${r.statusText} from ${r.url}${body ? `: ${body}` : ''}`);
  }
  return await r.json();
}

export async function apiGlobalSearch(q: string, limit = 10): Promise<GlobalSearchResponse> {
  const u = new URL(`${API_BASE_URL}/api/search`);
  u.searchParams.set('q', q);
  u.searchParams.set('limit', String(limit));
  return await fetchJson<GlobalSearchResponse>(u);
}

export async function apiProviderFilters(): Promise<any> {
  return await fetchJson(`${API_BASE_URL}/api/filters/providers`);
}

export async function apiProviderSearch(params: Record<string, string | string[] | number | undefined>): Promise<any> {
  const u = new URL(`${API_BASE_URL}/api/providers/search`);
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) {
      for (const item of v) u.searchParams.append(k, item);
    } else {
      u.searchParams.set(k, String(v));
    }
  }
  return await fetchJson(u);
}

export async function apiProviderDetail(npi: string): Promise<any> {
  return await fetchJson(`${API_BASE_URL}/api/providers/${encodeURIComponent(npi)}`);
}

export async function apiHcpcsSearch(params: Record<string, string | number | undefined>): Promise<any> {
  const u = new URL(`${API_BASE_URL}/api/hcpcs/search`);
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    u.searchParams.set(k, String(v));
  }
  return await fetchJson(u);
}

export async function apiHcpcsDetail(code: string): Promise<any> {
  return await fetchJson(`${API_BASE_URL}/api/hcpcs/${encodeURIComponent(code)}`);
}

export async function apiMapZips(params: Record<string, string | string[] | number | undefined>): Promise<any[]> {
  const u = new URL(`${API_BASE_URL}/api/map/zips`);
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) {
      for (const item of v) u.searchParams.append(k, item);
    } else {
      u.searchParams.set(k, String(v));
    }
  }
  return await fetchJson<any[]>(u);
}
