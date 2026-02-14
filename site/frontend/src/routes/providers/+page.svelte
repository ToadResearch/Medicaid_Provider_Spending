<script lang="ts">
  import { onMount } from 'svelte';
  import { apiProviderFilters, apiProviderSearch } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let filters: any = null;
  let loading = false;
  let err: string | null = null;

  let q = '';
  let role: 'total' | 'billing' | 'servicing' = 'total';
  let state: string[] = [];
  let entity = '';
  let taxonomy = '';
  let paidMin = '';
  let paidMax = '';
  let claimsMin = '';
  let claimsMax = '';
  let sort: string = 'paid_desc';
  let page = 0;
  const pageSize = 50;

  let res: any = null;

  onMount(async () => {
    try {
      filters = await apiProviderFilters();
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
    await run();
  });

  async function run() {
    err = null;
    loading = true;
    try {
      res = await apiProviderSearch({
        q: q.trim() ? q.trim() : undefined,
        role,
        state: state.length ? state : undefined,
        entity: entity || undefined,
        taxonomy: taxonomy ? [taxonomy] : undefined,
        paid_min: paidMin ? Number(paidMin) : undefined,
        paid_max: paidMax ? Number(paidMax) : undefined,
        claims_min: claimsMin ? Number(claimsMin) : undefined,
        claims_max: claimsMax ? Number(claimsMax) : undefined,
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

  function resetPaging() {
    page = 0;
  }
</script>

<div class="grid gap-6 lg:grid-cols-[320px_1fr]">
  <aside class="glass rounded-2xl p-5">
    <h1 class="font-serif text-2xl tracking-tight">Providers</h1>
    <p class="mt-2 text-xs text-white/60">
      Search by provider name, city, taxonomy description, or exact NPI. Toggle billing vs servicing role to change
      metrics and numeric filters.
    </p>

    <div class="mt-5 space-y-4 text-sm">
      <div>
        <label class="text-xs text-white/70" for="provider-q">Search</label>
        <input
          id="provider-q"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 placeholder:text-white/40 focus:outline-none"
          placeholder="e.g. pediatrics, Phoenix, 1922410166"
          bind:value={q}
          on:change={() => {
            resetPaging();
            void run();
          }}
        />
      </div>

      <div>
        <fieldset class="m-0 border-0 p-0">
          <legend class="text-xs text-white/70">Role</legend>
          <div class="mt-2 grid grid-cols-3 gap-2 text-xs" role="group" aria-label="Role">
          {#each ['total', 'billing', 'servicing'] as r}
            <button
              type="button"
              aria-pressed={role === r}
              class={`rounded-xl border px-2 py-2 ${role === r ? 'border-white/20 bg-white/15' : 'border-white/10 bg-white/5 hover:bg-white/10'}`}
              on:click={() => {
                role = r as any;
                resetPaging();
                void run();
              }}
            >
              {r}
            </button>
          {/each}
        </div>
        </fieldset>
      </div>

      <div>
        <label class="text-xs text-white/70" for="provider-state">State</label>
        <select
          id="provider-state"
          multiple
          class="mt-1 h-32 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
          bind:value={state}
          on:change={() => {
            resetPaging();
            void run();
          }}
        >
          {#each (filters?.states ?? []) as s}
            <option value={s}>{s}</option>
          {/each}
        </select>
      </div>

      <div>
        <label class="text-xs text-white/70" for="provider-entity">Entity type</label>
        <select
          id="provider-entity"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
          bind:value={entity}
          on:change={() => {
            resetPaging();
            void run();
          }}
        >
          <option value="">Any</option>
          {#each (filters?.entities ?? []) as e}
            <option value={e}>{e}</option>
          {/each}
        </select>
      </div>

      <div>
        <label class="text-xs text-white/70" for="provider-taxonomy">Primary taxonomy code</label>
        <input
          id="provider-taxonomy"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 placeholder:text-white/40 focus:outline-none"
          placeholder="e.g. 207Q00000X"
          bind:value={taxonomy}
          list="taxonomies"
          on:change={() => {
            resetPaging();
            void run();
          }}
        />
        <datalist id="taxonomies">
          {#each (filters?.taxonomies ?? []) as t}
            <option value={t.code}>{t.desc ?? ''}</option>
          {/each}
        </datalist>
      </div>

      <div class="grid grid-cols-2 gap-3">
        <div>
          <label class="text-xs text-white/70" for="provider-paid-min">Paid min</label>
          <input
            id="provider-paid-min"
            class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
            inputmode="numeric"
            placeholder="0"
            bind:value={paidMin}
            on:change={() => {
              resetPaging();
              void run();
            }}
          />
        </div>
        <div>
          <label class="text-xs text-white/70" for="provider-paid-max">Paid max</label>
          <input
            id="provider-paid-max"
            class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
            inputmode="numeric"
            placeholder="∞"
            bind:value={paidMax}
            on:change={() => {
              resetPaging();
              void run();
            }}
          />
        </div>
      </div>

      <div class="grid grid-cols-2 gap-3">
        <div>
          <label class="text-xs text-white/70" for="provider-claims-min">Claims min</label>
          <input
            id="provider-claims-min"
            class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
            inputmode="numeric"
            placeholder="0"
            bind:value={claimsMin}
            on:change={() => {
              resetPaging();
              void run();
            }}
          />
        </div>
        <div>
          <label class="text-xs text-white/70" for="provider-claims-max">Claims max</label>
          <input
            id="provider-claims-max"
            class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
            inputmode="numeric"
            placeholder="∞"
            bind:value={claimsMax}
            on:change={() => {
              resetPaging();
              void run();
            }}
          />
        </div>
      </div>

      <div>
        <label class="text-xs text-white/70" for="provider-sort">Sort</label>
        <select
          id="provider-sort"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
          bind:value={sort}
          on:change={() => {
            resetPaging();
            void run();
          }}
        >
          <option value="paid_desc">Paid (desc)</option>
          <option value="paid_asc">Paid (asc)</option>
          <option value="claims_desc">Claims (desc)</option>
          <option value="claims_asc">Claims (asc)</option>
          <option value="name_asc">Name (A-Z)</option>
          <option value="relevance">Relevance</option>
        </select>
      </div>
    </div>
  </aside>

  <section>
    <div class="flex items-center justify-between">
      <div class="text-xs text-white/60">
        {#if res}
          {res.total_hits} results
        {/if}
      </div>
      <div class="flex items-center gap-2">
        <button
          class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 disabled:opacity-40"
          disabled={page === 0 || loading}
          on:click={() => {
            page = Math.max(0, page - 1);
            void run();
          }}
        >
          Prev
        </button>
        <button
          class="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs hover:bg-white/10 disabled:opacity-40"
          disabled={loading || !res || (page + 1) * pageSize >= res.total_hits}
          on:click={() => {
            page = page + 1;
            void run();
          }}
        >
          Next
        </button>
      </div>
    </div>

    {#if err}
      <div class="mt-4 rounded-xl border border-red-400/20 bg-red-500/10 p-4 text-sm text-red-200">{err}</div>
    {/if}

    {#if loading}
      <div class="mt-4 text-sm text-white/60">Loading…</div>
    {:else if res}
      <div class="mt-4 space-y-2">
        {#each res.hits ?? [] as p}
          <a class="glass block rounded-2xl p-4 hover:bg-white/10" href={`/providers/${p.npi}`}>
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div class="min-w-0">
                <div class="truncate text-sm text-white/90">{p.display_name ?? p.npi}</div>
                <div class="truncate text-xs text-white/60">
                  {p.state ?? ''} {p.city ?? ''} {p.primary_taxonomy_desc ?? ''} <span class="text-white/40">({p.npi})</span>
                </div>
              </div>
              <div class="text-right text-xs text-white/70">
                <div>{fmtMoney(role === 'billing' ? p.paid_billing : role === 'servicing' ? p.paid_servicing : p.paid_total)}</div>
                <div class="text-white/50">
                  {fmtInt(role === 'billing' ? p.claims_billing : role === 'servicing' ? p.claims_servicing : p.claims_total)} claims
                </div>
              </div>
            </div>
          </a>
        {/each}
        {#if (res.hits ?? []).length === 0}
          <div class="mt-4 text-sm text-white/60">No results.</div>
        {/if}
      </div>
    {/if}
  </section>
</div>
