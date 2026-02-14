<script lang="ts">
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import { apiHcpcsDetail } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let code = '';
  $: code = $page.params.code;

  let loading = false;
  let err: string | null = null;
  let res: any = null;

  onMount(async () => {
    loading = true;
    try {
      res = await apiHcpcsDetail(code);
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
        <div class="font-serif text-2xl tracking-tight">{code}</div>
        <div class="mt-2 text-sm text-white/70">{res.hcpcs?.short_desc ?? res.hcpcs?.long_desc ?? ''}</div>
      </div>
      <div class="grid gap-2 text-right text-xs text-white/70">
        <div class="rounded-xl border border-white/10 bg-white/5 p-3">
          <div class="text-white/50">Total paid</div>
          <div class="text-sm text-white/90">{fmtMoney(Number(res.hcpcs?.paid_total ?? 0))}</div>
        </div>
        <div class="rounded-xl border border-white/10 bg-white/5 p-3">
          <div class="text-white/50">Total claims</div>
          <div class="text-sm text-white/90">{fmtInt(Number(res.hcpcs?.claims_total ?? 0))}</div>
        </div>
      </div>
    </div>

    <details class="mt-6 rounded-2xl border border-white/10 bg-white/5 p-4">
      <summary class="cursor-pointer text-sm text-white/80">Raw HCPCS API JSON</summary>
      <pre class="mt-3 max-h-[480px] overflow-auto whitespace-pre-wrap text-xs text-white/70">{res.hcpcs_api ?? ''}</pre>
    </details>
  </div>
{/if}

