(() => {
  const qs = (sel) => document.querySelector(sel);
  const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');

  const mappings = [
    { keys: ['qilin', 'bronzehighland'], lon: 103.8, lat: 35.9, place: 'China', country: 'China', region: 'APAC' },
    { keys: ['apt28', 'fancybear', 'sofacy', 'pawnstorm'], lon: 37.6, lat: 55.7, place: 'Russia', country: 'Russia', region: 'Europe' },
    { keys: ['apt29', 'cozybear', 'thenobelium'], lon: 37.6, lat: 55.7, place: 'Russia', country: 'Russia', region: 'Europe' },
    { keys: ['sandworm', 'voodoobear'], lon: 30.5, lat: 50.45, place: 'Ukraine theater', country: 'Ukraine', region: 'Europe' },
    { keys: ['lazarus', 'apt38', 'andariel'], lon: 127.0, lat: 39.0, place: 'North Korea', country: 'North Korea', region: 'APAC' },
    { keys: ['muddywater', 'seedworm'], lon: 51.4, lat: 35.7, place: 'Iran', country: 'Iran', region: 'Middle East' },
    { keys: ['charmingkitten', 'apt35', 'phosphorus'], lon: 51.4, lat: 35.7, place: 'Iran', country: 'Iran', region: 'Middle East' },
    { keys: ['oilrig', 'apt34', 'helixkitten'], lon: 51.4, lat: 35.7, place: 'Iran', country: 'Iran', region: 'Middle East' },
    { keys: ['volt typhoon', 'volttyphoon', 'flax typhoon', 'flaxtyphoon'], lon: 116.4, lat: 39.9, place: 'China', country: 'China', region: 'APAC' },
    { keys: ['mustangpanda', 'ta416', 'reddelta'], lon: 116.4, lat: 39.9, place: 'China', country: 'China', region: 'APAC' },
    { keys: ['carbanak', 'fin7', 'sangria tempest'], lon: 30.5, lat: 50.45, place: 'Eastern Europe', country: 'Ukraine', region: 'Europe' },
    { keys: ['lockbit'], lon: 37.6, lat: 55.7, place: 'Russia-linked', country: 'Russia', region: 'Europe' },
    { keys: ['conti'], lon: 30.5, lat: 50.45, place: 'Eastern Europe', country: 'Ukraine', region: 'Europe' },
    { keys: ['cl0p', 'clop'], lon: 30.5, lat: 50.45, place: 'Eastern Europe', country: 'Ukraine', region: 'Europe' },
    { keys: ['akira'], lon: -95.0, lat: 37.0, place: 'North America', country: 'United States', region: 'North America' },
    { keys: ['blackcat', 'alphv'], lon: -95.0, lat: 37.0, place: 'North America', country: 'United States', region: 'North America' },
    { keys: ['scatteredspider', '0ktapus'], lon: -95.0, lat: 37.0, place: 'North America', country: 'United States', region: 'North America' },
  ];

  const continentPolys = [
    { name: 'North America', points: [[-168,72],[-140,68],[-125,55],[-110,48],[-96,28],[-82,18],[-84,7],[-104,8],[-115,24],[-132,30],[-146,45],[-162,55]] },
    { name: 'South America', points: [[-80,10],[-70,5],[-64,-10],[-58,-22],[-60,-38],[-68,-52],[-76,-56],[-80,-40],[-82,-20]] },
    { name: 'Europe', points: [[-10,35],[5,44],[22,46],[34,44],[42,38],[48,30],[36,22],[25,20],[14,26],[2,28],[-6,32]] },
    { name: 'Middle East', points: [[34,40],[44,38],[56,35],[58,28],[52,20],[42,18],[36,24]] },
    { name: 'Africa', points: [[-18,30],[-4,28],[10,20],[20,10],[28,-4],[34,-16],[30,-30],[20,-34],[8,-24],[-2,-8],[-10,8],[-16,20]] },
    { name: 'APAC', points: [[58,22],[76,30],[95,40],[116,44],[130,42],[150,38],[160,28],[150,18],[130,16],[108,12],[92,10],[76,16]] },
    { name: 'Oceania', points: [[112,-12],[128,-16],[142,-24],[152,-33],[144,-42],[126,-40],[114,-30]] },
  ];

  const state = {
    actors: [],
    points: [],
    selectedRegion: '',
    selectedCountry: '',
    selectedActorId: '',
    selectedClusterActors: null,
    map: null,
    markersLayer: null,
  };

  function resolveActorGeo(actor) {
    const raw = String((actor && actor.display_name) || '').trim();
    const n = norm(raw);
    for (const m of mappings) {
      if (m.keys.some((k) => n.includes(norm(k)))) {
        return {
          id: String(actor.id || ''),
          name: raw,
          is_tracked: !!actor.is_tracked,
          notebook_status: String(actor.notebook_status || '').toLowerCase(),
          summary: String(actor.scope_statement || ''),
          lon: m.lon,
          lat: m.lat,
          place: m.place,
          country: m.country,
          region: m.region,
        };
      }
    }
    return null;
  }

  // Derive Active / Quiet / Dormant from notebook_status
  function actorStatus(point) {
    const ns = String(point.notebook_status || '').toLowerCase();
    if (ns === 'ready' || ns === 'running') return 'active';
    if (ns === 'warning') return 'quiet';
    return 'dormant';
  }

  // Dominant status for a cluster: loudest wins
  function clusterStatus(points) {
    if (points.some((p) => actorStatus(p) === 'active')) return 'active';
    if (points.some((p) => actorStatus(p) === 'quiet')) return 'quiet';
    return 'dormant';
  }

  function statusColor(status) {
    if (status === 'active') return '#ef4444';
    if (status === 'quiet') return '#f59e0b';
    return '#6b7280';
  }

  function pointInPoly(x, y, poly) {
    let inside = false;
    for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const xi = poly[i][0], yi = poly[i][1];
      const xj = poly[j][0], yj = poly[j][1];
      const hit = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-9) + xi);
      if (hit) inside = !inside;
    }
    return inside;
  }

  function continentFromLngLat(lng, lat) {
    const hit = continentPolys.find((c) => pointInPoly(lng, lat, c.points));
    return hit ? hit.name : '';
  }

  async function reverseCountry(lat, lng) {
    try {
      const url = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lng)}&zoom=4&addressdetails=1`;
      const resp = await fetch(url, {
        headers: { Accept: 'application/json', 'Accept-Language': 'en' },
      });
      if (!resp.ok) return '';
      const json = await resp.json();
      return String((json && json.address && json.address.country) || '').trim();
    } catch (_) {
      return '';
    }
  }

  function renderRegionList() {
    const title = qs('#geo-region-title');
    const hint = qs('#geo-region-hint');
    const list = qs('#geo-region-list');
    const detail = qs('#geo-actor-detail');
    if (!title || !hint || !list) return;

    let rows = [];
    if (state.selectedClusterActors) {
      rows = state.selectedClusterActors;
      title.textContent = `${rows[0].place} — ${rows.length} actors`;
      hint.textContent = 'Multiple actors mapped to this location. Select one to view details.';
    } else if (state.selectedCountry) {
      rows = state.points.filter((p) => String(p.country || '').toLowerCase() === state.selectedCountry.toLowerCase());
      title.textContent = `Country: ${state.selectedCountry}`;
      hint.textContent = rows.length
        ? 'Actors with confirmed mapping to this country.'
        : 'No mapped actors in this country. Try a nearby location or continent-level selection.';
    } else if (state.selectedRegion) {
      rows = state.points.filter((p) => p.region === state.selectedRegion);
      title.textContent = `Continent: ${state.selectedRegion}`;
      hint.textContent = rows.length
        ? 'Actors with confirmed mapping to this continent.'
        : 'No mapped actors currently in this continent.';
    } else {
      title.textContent = 'Select a location';
      hint.textContent = 'Click a country on the map. If country resolution fails, continent selection is used.';
      list.innerHTML = '';
      if (detail) detail.innerHTML = '<small>Select an actor to view details.</small>';
      return;
    }

    list.innerHTML = rows.map((r) => `
      <button type="button" class="geo-actor-pick" data-geo-actor-id="${r.id}">
        <strong>${r.name}</strong>
        <div class="meta">${r.place}</div>
      </button>
    `).join('');
    if (detail) detail.innerHTML = '<small>Select an actor to view details.</small>';
  }

  function renderActorDetail(actorId) {
    const detail = qs('#geo-actor-detail');
    if (!detail) return;
    const actor = state.points.find((p) => p.id === String(actorId || ''));
    if (!actor) {
      detail.innerHTML = '<small>Actor detail unavailable.</small>';
      return;
    }
    state.selectedActorId = actor.id;
    const summary = String(actor.summary || '').trim() || 'No summary available for this actor yet.';
    detail.innerHTML = `
      <div><strong>${actor.name}</strong></div>
      <div class="inline-note">${actor.country || actor.place} | ${actor.region}</div>
      <div style="margin-top:6px;font-size:12px;line-height:1.35">${summary}</div>
      <div style="margin-top:8px">
        ${actor.is_tracked
          ? '<span class="badge freshness-new">Tracked</span>'
          : `<button type="button" data-geo-track-id="${actor.id}">Add to tracked</button>`}
      </div>
    `;
  }

  function markerIcon(status, count) {
    const color = statusColor(status);
    if (count > 1) {
      return L.divIcon({
        className: 'geo-actor-marker geo-actor-cluster',
        html: `<div style="width:22px;height:22px;border-radius:50%;background:${color};border:2px solid rgba(255,255,255,0.35);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff;font-family:'Courier New',monospace;line-height:1">${count}</div>`,
        iconSize: [22, 22],
        iconAnchor: [11, 11],
      });
    }
    return L.divIcon({
      className: 'geo-actor-marker',
      html: `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:1px solid rgba(255,255,255,0.3);background:${color}"></span>`,
      iconSize: [12, 12],
      iconAnchor: [6, 6],
    });
  }

  function drawMarkers() {
    if (!state.map || !state.markersLayer) return;
    state.markersLayer.clearLayers();

    // Group actors by coordinate key
    const clusters = new Map();
    for (const p of state.points) {
      const key = `${p.lat},${p.lon}`;
      if (!clusters.has(key)) clusters.set(key, []);
      clusters.get(key).push(p);
    }

    for (const [, group] of clusters) {
      const p = group[0];
      const status = clusterStatus(group);
      const marker = L.marker([p.lat, p.lon], { icon: markerIcon(status, group.length) });

      if (group.length === 1) {
        marker.bindPopup(`<strong>${p.name}</strong><br>${p.place}`);
        marker.on('click', () => {
          state.selectedClusterActors = null;
          state.selectedCountry = p.country;
          state.selectedRegion = p.region;
          renderRegionList();
          renderActorDetail(p.id);
        });
      } else {
        const names = group.map((a) => a.name).join(', ');
        marker.bindPopup(`<strong>${group.length} actors</strong><br>${p.place}<br><small>${names}</small>`);
        marker.on('click', () => {
          state.selectedClusterActors = group;
          state.selectedCountry = '';
          state.selectedRegion = '';
          renderRegionList();
          if (qs('#geo-actor-detail')) qs('#geo-actor-detail').innerHTML = '<small>Select an actor to view details.</small>';
        });
      }

      state.markersLayer.addLayer(marker);
    }
  }

  async function fetchActors() {
    const resp = await fetch('/actors', { headers: { Accept: 'application/json' } });
    if (!resp.ok) throw new Error(`actors ${resp.status}`);
    const payload = await resp.json();
    const actors = Array.isArray(payload) ? payload : (Array.isArray(payload.actors) ? payload.actors : []);
    state.actors = actors;
    state.points = actors.map(resolveActorGeo).filter(Boolean);
  }

  async function trackActor(actorId) {
    const id = String(actorId || '').trim();
    if (!id) return;
    const resp = await fetch(`/actors/${encodeURIComponent(id)}/track`, { method: 'POST', headers: { Accept: 'application/json' } });
    if (!resp.ok) throw new Error(`track ${resp.status}`);
    for (const p of state.points) {
      if (p.id === id) p.is_tracked = true;
    }
    renderRegionList();
    renderActorDetail(id);
    drawMarkers();
  }

  async function handleMapPick(lat, lng) {
    state.selectedClusterActors = null;
    state.selectedCountry = '';
    state.selectedRegion = continentFromLngLat(lng, lat);
    renderRegionList();

    const hint = qs('#geo-region-hint');
    if (hint) hint.textContent = 'Resolving country selection...';

    const country = await reverseCountry(lat, lng);
    if (country) {
      state.selectedCountry = country;
      renderRegionList();
    } else {
      renderRegionList();
      if (hint) hint.textContent = 'Country lookup unavailable. Showing continent-level results.';
    }
  }

  function ensureMap() {
    if (state.map) return;
    const mapEl = qs('#geo-map-canvas');
    if (!mapEl) return;
    if (!window.L) {
      mapEl.innerHTML = '<div style="padding:12px;color:#1f2937;font-size:13px">Map library could not load in this browser session. Check network/CSP and reload.</div>';
      return;
    }

    state.map = L.map(mapEl, {
      worldCopyJump: true,
      minZoom: 2,
      maxZoom: 6,
      zoomControl: true,
    }).setView([20, 10], 2);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      maxZoom: 19,
    }).addTo(state.map);

    state.markersLayer = L.layerGroup().addTo(state.map);
    drawMarkers();

    state.map.on('click', async (evt) => {
      const lat = Number(evt.latlng.lat || 0);
      const lng = Number(evt.latlng.lng || 0);
      await handleMapPick(lat, lng);
    });
  }

  function openModal() {
    const modal = qs('#geo-modal');
    if (!modal) return;
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    state.selectedRegion = '';
    state.selectedCountry = '';
    state.selectedClusterActors = null;
    renderRegionList();
    ensureMap();
    if (state.map) setTimeout(() => state.map.invalidateSize(), 0);
  }

  function closeModal() {
    const modal = qs('#geo-modal');
    if (!modal) return;
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
  }

  function bindEvents() {
    const openBtn = qs('#geo-map-open');
    const closeBtn = qs('#geo-map-close');
    const modal = qs('#geo-modal');
    const list = qs('#geo-region-list');

    if (openBtn) openBtn.addEventListener('click', openModal);
    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (modal) {
      modal.addEventListener('click', (e) => {
        if (e.target && e.target.id === 'geo-modal') closeModal();
      });
    }
    if (list) {
      list.addEventListener('click', async (e) => {
        const pick = e.target.closest('[data-geo-actor-id]');
        if (pick) {
          renderActorDetail(pick.getAttribute('data-geo-actor-id') || '');
          return;
        }
        const btn = e.target.closest('[data-geo-track-id]');
        if (!btn) return;
        const actorId = btn.getAttribute('data-geo-track-id') || '';
        btn.disabled = true;
        try {
          await trackActor(actorId);
        } catch (err) {
          btn.disabled = false;
          window.alert(`Could not track actor: ${err.message}`);
        }
      });
    }
    const detail = qs('#geo-actor-detail');
    if (detail) {
      detail.addEventListener('click', async (e) => {
        const btn = e.target.closest('[data-geo-track-id]');
        if (!btn) return;
        const actorId = btn.getAttribute('data-geo-track-id') || '';
        btn.disabled = true;
        try {
          await trackActor(actorId);
        } catch (err) {
          btn.disabled = false;
          window.alert(`Could not track actor: ${err.message}`);
        }
      });
    }
  }

  async function init() {
    if (!qs('#geo-map-open')) return;
    try {
      await fetchActors();
      bindEvents();
    } catch (err) {
      console.error('Geography map init failed:', err);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
