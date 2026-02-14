<script lang="ts">
  import { page } from '$app/stores';
  import { apiGlobalSearch } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let q = '';
  $: q = $page.url.searchParams.get('q') ?? '';

  let loading = false;
  let err: string | null = null;
  let res: any = null;

  async function run() {
    err = null;
    res = null;
    const term = q.trim();
    if (!term) return;
    loading = true;
    try {
      res = await apiGlobalSearch(term, 10);
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  }

  $: void run();
</script>

<section class="mb-10">
  <div class="glass rounded-2xl p-6">
    <h1 class="font-serif text-3xl tracking-tight">Explore provider spending, fast.</h1>
    <p class="mt-2 max-w-3xl text-sm text-white/70">
      Free-text search across providers (name, NPI, city, taxonomy) and HCPCS descriptions. Filters and a ZIP-centroid
      heatmap are available in the dedicated pages.
    </p>
    <div class="mt-4 flex flex-wrap gap-2 text-xs text-white/70">
      <span class="chip rounded-full px-3 py-1">Providers</span>
      <span class="chip rounded-full px-3 py-1">Billing vs servicing roles</span>
      <span class="chip rounded-full px-3 py-1">HCPCS descriptions</span>
      <span class="chip rounded-full px-3 py-1">Map (ZIP centroids)</span>
    </div>
  </div>
</section>

{#if q.trim().length === 0}
  <div class="text-sm text-white/60">Type a search in the top bar to begin.</div>
{:else}
  {#if loading}
    <div class="text-sm text-white/60">Searching…</div>
  {:else if err}
    <div class="text-sm text-red-200">{err}</div>
  {:else if res}
    <div class="grid gap-6 md:grid-cols-2">
      <section class="glass rounded-2xl p-5">
        <h2 class="text-sm font-medium text-white/80">Providers</h2>
        <div class="mt-3 space-y-2">
          {#each res.providers ?? [] as p}
            <a class="block rounded-xl border border-white/10 bg-white/5 p-3 hover:bg-white/10" href={`/providers/${p.npi}`}>
              <div class="flex items-baseline justify-between gap-3">
                <div class="min-w-0">
                  <div class="truncate text-sm text-white/90">{p.display_name ?? p.npi}</div>
                  <div class="truncate text-xs text-white/60">{p.state ?? ''} {p.city ?? ''} {p.primary_taxonomy_desc ?? ''}</div>
                </div>
                <div class="text-right text-xs text-white/70">
                  <div>{fmtMoney(p.paid_total)}</div>
                  <div class="text-white/50">{fmtInt(p.claims_total)} claims</div>
                </div>
              </div>
            </a>
          {/each}
          {#if (res.providers ?? []).length === 0}
            <div class="text-xs text-white/50">No provider matches.</div>
          {/if}
        </div>
      </section>

      <section class="glass rounded-2xl p-5">
        <h2 class="text-sm font-medium text-white/80">HCPCS</h2>
        <div class="mt-3 space-y-2">
          {#each res.hcpcs ?? [] as h}
            <a class="block rounded-xl border border-white/10 bg-white/5 p-3 hover:bg-white/10" href={`/hcpcs/${h.hcpcs_code}`}>
              <div class="flex items-baseline justify-between gap-3">
                <div class="min-w-0">
                  <div class="truncate text-sm text-white/90">{h.hcpcs_code}</div>
                  <div class="truncate text-xs text-white/60">{h.short_desc ?? h.long_desc ?? ''}</div>
                </div>
                <div class="text-right text-xs text-white/70">
                  <div>{fmtMoney(h.paid_total)}</div>
                  <div class="text-white/50">{fmtInt(h.claims_total)} claims</div>
                </div>
              </div>
            </a>
          {/each}
          {#if (res.hcpcs ?? []).length === 0}
            <div class="text-xs text-white/50">No HCPCS matches.</div>
          {/if}
        </div>
      </section>
    </div>
  {/if}
{/if}

