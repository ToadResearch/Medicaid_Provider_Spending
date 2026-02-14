<script lang="ts">
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import { apiProviderDetail } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let npi = '';
  $: npi = $page.params.npi;

  let loading = false;
  let err: string | null = null;
  let res: any = null;

  onMount(async () => {
    loading = true;
    try {
      res = await apiProviderDetail(npi);
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  });
</script>

{#if loading}
  <div class="text-sm text-white/60">Loading…</div>
{:else if err}
  <div class="rounded-xl border border-red-400/20 bg-red-500/10 p-4 text-sm text-red-200">{err}</div>
{:else if res}
  <div class="glass rounded-2xl p-6">
    <div class="flex flex-wrap items-start justify-between gap-4">
      <div class="min-w-0">
        <div class="font-serif text-2xl tracking-tight">{res.provider?.display_name ?? npi}</div>
        <div class="mt-1 text-sm text-white/60">
          <span class="chip rounded-full px-3 py-1 text-xs">{npi}</span>
          {#if res.provider?.enumeration_type}
            <span class="chip ml-2 rounded-full px-3 py-1 text-xs">{res.provider.enumeration_type}</span>
          {/if}
          {#if res.provider?.primary_taxonomy_code}
            <span class="chip ml-2 rounded-full px-3 py-1 text-xs">{res.provider.primary_taxonomy_code}</span>
          {/if}
        </div>
        <div class="mt-2 text-sm text-white/70">
          {res.provider?.city ?? ''} {res.provider?.state ?? ''} {res.provider?.zip5 ?? ''}
        </div>
      </div>
      <div class="grid gap-2 text-right text-xs text-white/70">
        <div class="rounded-xl border border-white/10 bg-white/5 p-3">
          <div class="text-white/50">Total paid</div>
          <div class="text-sm text-white/90">{fmtMoney(Number(res.provider?.paid_total ?? 0))}</div>
        </div>
        <div class="rounded-xl border border-white/10 bg-white/5 p-3">
          <div class="text-white/50">Total claims</div>
          <div class="text-sm text-white/90">{fmtInt(Number(res.provider?.claims_total ?? 0))}</div>
        </div>
      </div>
    </div>

    <div class="mt-6 grid gap-4 md:grid-cols-3">
      <div class="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div class="text-xs text-white/50">Billing paid</div>
        <div class="mt-1 text-sm text-white/90">{fmtMoney(Number(res.provider?.paid_billing ?? 0))}</div>
        <div class="mt-1 text-xs text-white/50">{fmtInt(Number(res.provider?.claims_billing ?? 0))} claims</div>
      </div>
      <div class="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div class="text-xs text-white/50">Servicing paid</div>
        <div class="mt-1 text-sm text-white/90">{fmtMoney(Number(res.provider?.paid_servicing ?? 0))}</div>
        <div class="mt-1 text-xs text-white/50">{fmtInt(Number(res.provider?.claims_servicing ?? 0))} claims</div>
      </div>
      <div class="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div class="text-xs text-white/50">Beneficiaries (total)</div>
        <div class="mt-1 text-sm text-white/90">{fmtInt(Number(res.provider?.bene_total ?? 0))}</div>
      </div>
    </div>

    <details class="mt-6 rounded-2xl border border-white/10 bg-white/5 p-4">
      <summary class="cursor-pointer text-sm text-white/80">Raw NPI API JSON</summary>
      <pre class="mt-3 max-h-[480px] overflow-auto whitespace-pre-wrap text-xs text-white/70">{res.npi_api ?? ''}</pre>
    </details>
  </div>
{/if}

