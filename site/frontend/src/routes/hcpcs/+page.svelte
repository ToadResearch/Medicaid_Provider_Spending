<script lang="ts">
  import { onMount } from 'svelte';
  import { apiHcpcsSearch } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let q = '';
  let sort: string = 'paid_desc';
  let page = 0;
  const pageSize = 50;

  let loading = false;
  let err: string | null = null;
  let res: any = null;

  onMount(async () => {
    await run();
  });

  async function run() {
    err = null;
    loading = true;
    try {
      res = await apiHcpcsSearch({
        q: q.trim() ? q.trim() : undefined,
        sort,
        page,
        page_size: pageSize
      });
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  }
</script>

<div class="glass rounded-2xl p-6">
  <div class="flex flex-wrap items-center justify-between gap-4">
    <div>
      <h1 class="font-serif text-2xl tracking-tight">HCPCS</h1>
      <p class="mt-2 text-xs text-white/60">Free-text search on HCPCS code + descriptions. Sorted by rollup totals.</p>
    </div>
    <div class="flex flex-wrap gap-2">
      <input
        class="w-72 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 placeholder:text-white/40 focus:outline-none"
        placeholder="e.g. T1019, personal care"
        bind:value={q}
        on:keydown={(e) => {
          if (e.key === 'Enter') {
            page = 0;
            void run();
          }
        }}
      />
      <select
        class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
        bind:value={sort}
        on:change={() => {
          page = 0;
          void run();
        }}
      >
        <option value="paid_desc">Paid (desc)</option>
        <option value="paid_asc">Paid (asc)</option>
        <option value="claims_desc">Claims (desc)</option>
        <option value="claims_asc">Claims (asc)</option>
        <option value="relevance">Relevance</option>
      </select>
      <button
        class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm hover:bg-white/10"
        on:click={() => {
          page = 0;
          void run();
        }}
      >
        Go
      </button>
    </div>
  </div>

  {#if err}
    <div class="mt-4 rounded-xl border border-red-400/20 bg-red-500/10 p-4 text-sm text-red-200">{err}</div>
  {/if}

  {#if loading}
    <div class="mt-4 text-sm text-white/60">Loading…</div>
  {:else if res}
    <div class="mt-4 flex items-center justify-between text-xs text-white/60">
      <div>{res.total_hits} results</div>
      <div class="flex items-center gap-2">
        <button
          class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 disabled:opacity-40"
          disabled={page === 0}
          on:click={() => {
            page = Math.max(0, page - 1);
            void run();
          }}
        >
          Prev
        </button>
        <button
          class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 disabled:opacity-40"
          disabled={(page + 1) * pageSize >= res.total_hits}
          on:click={() => {
            page = page + 1;
            void run();
          }}
        >
          Next
        </button>
      </div>
    </div>

    <div class="mt-4 space-y-2">
      {#each res.hits ?? [] as h}
        <a class="block rounded-2xl border border-white/10 bg-white/5 p-4 hover:bg-white/10" href={`/hcpcs/${h.hcpcs_code}`}>
          <div class="flex flex-wrap items-center justify-between gap-3">
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
      {#if (res.hits ?? []).length === 0}
        <div class="mt-4 text-sm text-white/60">No results.</div>
      {/if}
    </div>
  {/if}
</div>

