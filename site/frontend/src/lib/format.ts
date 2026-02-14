export function fmtMoney(n: number | null | undefined): string {
  if (n === null || n === undefined) return '-';
  if (!Number.isFinite(n)) return '-';
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n);
}

export function fmtInt(n: number | null | undefined): string {
  if (n === null || n === undefined) return '-';
  if (!Number.isFinite(n)) return '-';
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(n);
}

