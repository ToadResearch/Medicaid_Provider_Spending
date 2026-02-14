<script lang="ts">
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';

  let q = '';
  $: q = $page.url.searchParams.get('q') ?? '';

  function onSubmit(e: SubmitEvent) {
    e.preventDefault();
    const form = e.currentTarget as HTMLFormElement;
    const fd = new FormData(form);
    const v = String(fd.get('q') ?? '').trim();
    goto(v ? `/?q=${encodeURIComponent(v)}` : '/');
  }
</script>

<header class="sticky top-0 z-50 border-b border-white/10 bg-black/20 backdrop-blur">
  <div class="mx-auto flex max-w-6xl items-center gap-4 px-4 py-3">
    <a href="/" class="flex items-baseline gap-2">
      <span class="font-serif text-lg tracking-tight">Spending Explorer</span>
      <span class="text-xs text-white/60">Medicaid Provider Spending</span>
    </a>

    <nav class="ml-4 hidden items-center gap-3 text-sm text-white/70 md:flex">
      <a class="hover:text-white" href="/providers">Providers</a>
      <a class="hover:text-white" href="/hcpcs">HCPCS</a>
      <a class="hover:text-white" href="/map">Map</a>
    </nav>

    <form on:submit={onSubmit} class="ml-auto flex w-full max-w-xl items-center gap-2">
      <div class="glass flex w-full items-center gap-2 rounded-xl px-3 py-2">
        <input
          name="q"
          class="w-full bg-transparent text-sm text-white/90 placeholder:text-white/40 focus:outline-none"
          placeholder="Search provider name, NPI, city, taxonomy, or HCPCS..."
          value={q}
        />
        <button
          type="submit"
          class="rounded-lg border border-white/15 bg-white/10 px-3 py-1 text-xs text-white/85 hover:bg-white/15"
        >
          Search
        </button>
      </div>
    </form>
  </div>
</header>

