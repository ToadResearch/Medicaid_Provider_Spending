<script lang="ts">
  import { onMount } from 'svelte';
  import maplibregl from 'maplibre-gl';
  import { apiMapZips, apiProviderFilters } from '$lib/api';
  import { fmtInt, fmtMoney } from '$lib/format';

  let mapEl: HTMLDivElement | null = null;
  let map: maplibregl.Map | null = null;
  let filters: any = null;
  let err: string | null = null;

  let role: 'total' | 'billing' | 'servicing' = 'total';
  let metric: 'paid' | 'claims' | 'bene' = 'paid';
  let state: string[] = [];
  let entity = '';
  let taxonomy = '';

  let lastFetch = 0;

  function bboxString(): string {
    if (!map) return '';
    const b = map.getBounds();
    return `${b.getWest()},${b.getSouth()},${b.getEast()},${b.getNorth()}`;
  }

  async function refresh() {
    if (!map) return;
    const now = Date.now();
    if (now - lastFetch < 250) return;
    lastFetch = now;

    err = null;
    try {
      const points = await apiMapZips({
        bbox: bboxString(),
        role,
        metric,
        state: state.length ? state : undefined,
        entity: entity || undefined,
        taxonomy: taxonomy ? [taxonomy] : undefined
      });

      const fc = {
        type: 'FeatureCollection',
        features: points.map((p: any) => ({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [p.lon, p.lat] },
          properties: {
            zip5: p.zip5,
            provider_count: p.provider_count,
            metric_total: p.metric_total
          }
        }))
      } as any;

      const src: any = map.getSource('zips');
      if (src) src.setData(fc);
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
  }

  onMount(async () => {
    try {
      filters = await apiProviderFilters();
    } catch (e: any) {
      err = e?.message ?? String(e);
    }

    if (!mapEl) return;
    map = new maplibregl.Map({
      container: mapEl,
      style: (import.meta.env.VITE_MAP_STYLE_URL as string) ?? 'https://demotiles.maplibre.org/style.json',
      center: [-98.5, 39.8],
      zoom: 3.6
    });

    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-right');

    map.on('load', async () => {
      map!.addSource('zips', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
      });

      map!.addLayer({
        id: 'zips-heat',
        type: 'heatmap',
        source: 'zips',
        maxzoom: 8,
        paint: {
          'heatmap-weight': ['interpolate', ['linear'], ['get', 'metric_total'], 0, 0, 500000, 1],
          'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 3, 1, 8, 3],
          'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 3, 6, 8, 24],
          'heatmap-color': [
            'interpolate',
            ['linear'],
            ['heatmap-density'],
            0,
            'rgba(0,0,0,0)',
            0.15,
            'rgba(122,167,255,0.35)',
            0.45,
            'rgba(105,240,210,0.45)',
            0.75,
            'rgba(255,202,107,0.65)',
            1,
            'rgba(255,80,110,0.85)'
          ]
        }
      });

      map!.addLayer({
        id: 'zips-points',
        type: 'circle',
        source: 'zips',
        minzoom: 7.5,
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 7.5, 2, 11, 6],
          'circle-color': 'rgba(105,240,210,0.75)',
          'circle-stroke-color': 'rgba(0,0,0,0.35)',
          'circle-stroke-width': 1
        }
      });

      map!.on('moveend', () => void refresh());
      map!.on('click', 'zips-points', (e: any) => {
        const f = e.features?.[0];
        if (!f) return;
        const props = f.properties ?? {};
        const html = `
          <div style="font-family: IBM Plex Sans, sans-serif; font-size: 12px; line-height: 1.35;">
            <div style="font-weight: 600; margin-bottom: 4px;">ZIP ${props.zip5}</div>
            <div style="opacity: 0.85;">Providers: ${props.provider_count}</div>
            <div style="opacity: 0.85;">Metric total: ${Number(props.metric_total).toLocaleString()}</div>
            <div style="opacity: 0.55; margin-top: 6px;">Coordinates are ZIP centroids (approx).</div>
          </div>
        `;
        new maplibregl.Popup({ closeButton: true })
          .setLngLat(e.lngLat)
          .setHTML(html)
          .addTo(map!);
      });

      await refresh();
    });
  });
</script>

<div class="grid gap-6 lg:grid-cols-[360px_1fr]">
  <aside class="glass rounded-2xl p-5">
    <h1 class="font-serif text-2xl tracking-tight">Map</h1>
    <p class="mt-2 text-xs text-white/60">
      Heatmap and points are built from provider practice ZIP centroids (approximate). Use filters to focus the view.
    </p>

    <div class="mt-5 space-y-4 text-sm">
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
                void refresh();
              }}
            >
              {r}
            </button>
          {/each}
        </div>
        </fieldset>
      </div>

      <div>
        <fieldset class="m-0 border-0 p-0">
          <legend class="text-xs text-white/70">Metric</legend>
          <div class="mt-2 grid grid-cols-3 gap-2 text-xs" role="group" aria-label="Metric">
          {#each ['paid', 'claims', 'bene'] as m}
            <button
              type="button"
              aria-pressed={metric === m}
              class={`rounded-xl border px-2 py-2 ${metric === m ? 'border-white/20 bg-white/15' : 'border-white/10 bg-white/5 hover:bg-white/10'}`}
              on:click={() => {
                metric = m as any;
                void refresh();
              }}
            >
              {m}
            </button>
          {/each}
        </div>
        </fieldset>
      </div>

      <div>
        <label class="text-xs text-white/70" for="map-state">State</label>
        <select
          id="map-state"
          multiple
          class="mt-1 h-32 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
          bind:value={state}
          on:change={() => void refresh()}
        >
          {#each (filters?.states ?? []) as s}
            <option value={s}>{s}</option>
          {/each}
        </select>
      </div>

      <div>
        <label class="text-xs text-white/70" for="map-entity">Entity type</label>
        <select
          id="map-entity"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 focus:outline-none"
          bind:value={entity}
          on:change={() => void refresh()}
        >
          <option value="">Any</option>
          {#each (filters?.entities ?? []) as e}
            <option value={e}>{e}</option>
          {/each}
        </select>
      </div>

      <div>
        <label class="text-xs text-white/70" for="map-taxonomy">Primary taxonomy code</label>
        <input
          id="map-taxonomy"
          class="mt-1 w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/90 placeholder:text-white/40 focus:outline-none"
          placeholder="e.g. 207Q00000X"
          bind:value={taxonomy}
          list="taxonomies"
          on:change={() => void refresh()}
        />
        <datalist id="taxonomies">
          {#each (filters?.taxonomies ?? []) as t}
            <option value={t.code}>{t.desc ?? ''}</option>
          {/each}
        </datalist>
      </div>

      {#if err}
        <div class="whitespace-pre-wrap rounded-xl border border-red-400/20 bg-red-500/10 p-3 text-xs text-red-200">
          {err}
        </div>
      {/if}
    </div>

    <div class="mt-6 text-xs text-white/50">
      Tip: Zoom in to see individual points. This is a ZIP centroid view, not exact geocoding.
    </div>
  </aside>

  <section class="glass rounded-2xl p-2">
    <div bind:this={mapEl} class="h-[70vh] w-full overflow-hidden rounded-2xl"></div>
  </section>
</div>
