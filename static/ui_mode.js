(() => {
  const KEY = 'actorwatch:ui-mode:v1';
  const MODES = new Set(['classic', 'redraw']);
  const SELECT_ID = 'ui-mode-select';

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
        ? 'Redraw mode active. This is a higher-contrast operations-style layout.'
        : 'Classic mode active. This keeps the standard ActorWatch visual layout.';
    }
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
