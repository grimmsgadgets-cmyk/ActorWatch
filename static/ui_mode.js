(() => {
  const KEY = 'actorwatch:ui-mode:v1';
  const MODES = new Set(['classic', 'redraw', 'bastion']);
  const SELECT_ID = 'ui-mode-select';
  let bastionTick = null;

  function clickIfPresent(id) {
    const el = document.getElementById(id);
    if (el) el.click();
  }

  function appendBastionLog(line) {
    const log = document.getElementById('bastion-terminal-log');
    if (!log) return;
    const existing = String(log.textContent || '').trimEnd();
    const stamp = new Date().toLocaleTimeString();
    const next = `${existing}\n[${stamp}] ${line}`;
    const rows = next.split('\n');
    log.textContent = rows.slice(-14).join('\n');
    log.scrollTop = log.scrollHeight;
  }

  function setupBastionDeck() {
    const shell = document.getElementById('bastion-shell');
    if (!shell) return;

    const bind = (id, fn) => {
      const el = document.getElementById(id);
      if (!el || el.dataset.bound === '1') return;
      el.dataset.bound = '1';
      el.addEventListener('click', fn);
    };

    bind('bastion-cmd-map', () => { clickIfPresent('geo-map-open'); appendBastionLog('Opened geography map.'); });
    bind('bastion-cmd-refresh', () => { clickIfPresent('refresh-actor-button'); appendBastionLog('Refresh actor requested.'); });
    bind('bastion-cmd-terminal-notes', () => { clickIfPresent('terminal-add-note'); appendBastionLog('Add note dialog opened.'); });
    bind('bastion-cmd-terminal-gennotes', () => { clickIfPresent('terminal-generate-notes'); appendBastionLog('Generate notes requested.'); });
    bind('bastion-cmd-terminal-timeline', () => {
      clickIfPresent('open-timeline-details-link');
      appendBastionLog('Opened timeline details.');
    });
    bind('bastion-cmd-terminal-top', () => { window.scrollTo({ top: 0, behavior: 'smooth' }); appendBastionLog('Scrolled to top.'); });

    const sync = () => {
      const status = document.getElementById('notebook-health-message');
      const sources = document.getElementById('notebook-health-sources');
      const activity = document.getElementById('recent-reports');
      const techniques = document.getElementById('top-techniques');
      const statusOut = document.getElementById('bastion-k-status');
      const sourcesOut = document.getElementById('bastion-k-sources');
      const activityOut = document.getElementById('bastion-k-activity');
      const techniquesOut = document.getElementById('bastion-k-techniques');
      if (statusOut && status) statusOut.textContent = String(status.textContent || '').trim() || statusOut.textContent;
      if (sourcesOut && sources) sourcesOut.textContent = String(sources.textContent || '').trim() || sourcesOut.textContent;
      if (activityOut && activity) activityOut.textContent = String(activity.textContent || '').trim() || activityOut.textContent;
      if (techniquesOut && techniques) techniquesOut.textContent = String(techniques.textContent || '').trim() || techniquesOut.textContent;
    };

    sync();
    if (!bastionTick) bastionTick = window.setInterval(sync, 5000);
  }

  function teardownBastionDeck() {
    if (bastionTick) {
      window.clearInterval(bastionTick);
      bastionTick = null;
    }
  }

  function currentMode() {
    const stored = String(localStorage.getItem(KEY) || '').trim().toLowerCase();
    if (MODES.has(stored)) return stored;
    return 'classic';
  }

  function applyMode(mode) {
    const m = MODES.has(mode) ? mode : 'classic';
    document.body.dataset.uiMode = m;
    document.querySelectorAll('[data-ui-mode-choice]').forEach((btn) => {
      const active = String(btn.getAttribute('data-ui-mode-choice') || '') === m;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
    const select = document.getElementById(SELECT_ID);
    if (select && String(select.value || '') !== m) {
      select.value = m;
    }
    const status = document.getElementById('ui-mode-status');
    if (status) {
      status.textContent = m === 'redraw'
        ? 'Redraw mode active. Green-tint operations layout enabled.'
        : m === 'bastion'
          ? 'Bastion mode active. Immersive command-deck layout enabled.'
          : 'Classic mode active. This keeps the standard ActorWatch visual layout.';
    }
    if (m === 'bastion') setupBastionDeck();
    else teardownBastionDeck();
    return m;
  }

  function setMode(mode) {
    const applied = applyMode(mode);
    localStorage.setItem(KEY, applied);
  }

  function init() {
    if (!document.body) return;
    applyMode(currentMode());

    document.querySelectorAll('[data-ui-mode-choice]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const mode = String(btn.getAttribute('data-ui-mode-choice') || '').trim().toLowerCase();
        setMode(mode);
      });
    });
    const select = document.getElementById(SELECT_ID);
    if (select) {
      select.addEventListener('change', () => {
        const mode = String(select.value || '').trim().toLowerCase();
        setMode(mode);
      });
    }

    window.addEventListener('storage', (evt) => {
      if (evt.key === KEY) applyMode(currentMode());
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
