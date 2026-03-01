(() => {
  const KEY = 'actorwatch:ui-mode:v1';
  const MODES = new Set(['classic', 'redraw', 'bastion']);
  const SELECT_ID = 'ui-mode-select';
  let bastionTick = null;
  let bastionKeyHandler = null;

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

  function dispatchBastionCmd(raw) {
    const cmd = String(raw || '').trim().toLowerCase();
    const rawTrimmed = String(raw || '').trim();
    if (!cmd) return;
    if (cmd === 'help') {
      appendBastionLog('Commands: help  refresh  note <text>  timeline  status  clear  map');
      appendBastionLog('Keys:     [R] refresh  [N] note  [T] timeline  [G] map');
      return;
    }
    if (cmd === 'refresh' || cmd === 'r') {
      clickIfPresent('refresh-actor-button');
      appendBastionLog('Refresh actor requested.');
      return;
    }
    if (cmd.startsWith('note ')) {
      const text = rawTrimmed.slice(5).trim();
      clickIfPresent('terminal-add-note');
      appendBastionLog(text ? `Note dialog opened: "${text.slice(0, 40)}"` : 'Add note dialog opened.');
      return;
    }
    if (cmd === 'note') {
      clickIfPresent('terminal-add-note');
      appendBastionLog('Add note dialog opened.');
      return;
    }
    if (cmd === 'timeline' || cmd === 't') {
      clickIfPresent('open-timeline-details-link');
      appendBastionLog('Opened timeline details.');
      return;
    }
    if (cmd === 'map' || cmd === 'g') {
      clickIfPresent('geo-map-open');
      appendBastionLog('Opened geography map.');
      return;
    }
    if (cmd === 'status') {
      const statusEl = document.getElementById('notebook-health-message');
      const sourcesEl = document.getElementById('notebook-health-sources');
      const s = statusEl ? statusEl.textContent.trim() : 'N/A';
      const src = sourcesEl ? sourcesEl.textContent.trim() : 'N/A';
      appendBastionLog(`Status: ${s} | Sources: ${src}`);
      return;
    }
    if (cmd === 'clear') {
      const log = document.getElementById('bastion-terminal-log');
      if (log) log.textContent = '';
      appendBastionLog('Terminal cleared.');
      return;
    }
    appendBastionLog(`Unknown command: "${cmd}". Type "help" for available commands.`);
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

    bind('bastion-cmd-exit', () => { setMode('classic'); });
    bind('bastion-cmd-map', () => { clickIfPresent('geo-map-open'); appendBastionLog('Opened geography map.'); });
    bind('bastion-cmd-refresh', () => { clickIfPresent('refresh-actor-button'); appendBastionLog('Refresh actor requested.'); });

    const cmdInput = document.getElementById('bastion-cmd-input');
    if (cmdInput && !cmdInput.dataset.bound) {
      cmdInput.dataset.bound = '1';
      cmdInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          const val = cmdInput.value;
          cmdInput.value = '';
          dispatchBastionCmd(val);
        }
      });
    }

    if (!bastionKeyHandler) {
      bastionKeyHandler = (e) => {
        if (document.body.dataset.uiMode !== 'bastion') return;
        const tag = (e.target.tagName || '').toUpperCase();
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
        if (e.ctrlKey || e.altKey || e.metaKey) return;
        const key = e.key.toLowerCase();
        if (key === 'r') { e.preventDefault(); clickIfPresent('refresh-actor-button'); appendBastionLog('Refresh requested. [R]'); }
        else if (key === 'n') { e.preventDefault(); clickIfPresent('terminal-add-note'); appendBastionLog('Add note dialog opened. [N]'); }
        else if (key === 't') { e.preventDefault(); clickIfPresent('open-timeline-details-link'); appendBastionLog('Timeline opened. [T]'); }
        else if (key === 'g') { e.preventDefault(); clickIfPresent('geo-map-open'); appendBastionLog('Geography map opened. [G]'); }
      };
      document.addEventListener('keydown', bastionKeyHandler);
    }

    const ping = document.getElementById('bastion-terminal-ping');
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
      if (ping) {
        ping.textContent = '● LIVE';
        ping.classList.add('live');
        setTimeout(() => { ping.classList.remove('live'); ping.textContent = 'ONLINE'; }, 1200);
      }
    };

    sync();
    if (!bastionTick) bastionTick = window.setInterval(sync, 5000);
  }

  function teardownBastionDeck() {
    if (bastionTick) {
      window.clearInterval(bastionTick);
      bastionTick = null;
    }
    if (bastionKeyHandler) {
      document.removeEventListener('keydown', bastionKeyHandler);
      bastionKeyHandler = null;
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
