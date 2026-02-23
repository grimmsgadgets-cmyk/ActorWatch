(function () {
  const form = document.getElementById("add-actor-form");
  const input = document.getElementById("add-actor-input");
  const list = document.getElementById("actor-autocomplete-list");
  const hint = document.getElementById("actor-autocomplete-hint");
  if (!form || !input || !list) return;

  let actors = [];
  let suggestions = [];
  let activeIndex = -1;
  let selectedActorId = "";

  function normalize(value) {
    return String(value || "").trim().toLowerCase().replace(/\s+/g, " ");
  }

  function hideList() {
    list.hidden = true;
    list.innerHTML = "";
    suggestions = [];
    activeIndex = -1;
  }

  function setHint(text) {
    if (!hint) return;
    hint.textContent = text;
  }

  function exactMatchByName(name) {
    const needle = normalize(name);
    if (!needle) return null;
    for (const actor of actors) {
      if (normalize(actor.display_name) === needle) return actor;
    }
    return null;
  }

  function applySuggestion(actor) {
    if (!actor) return;
    input.value = actor.display_name;
    selectedActorId = String(actor.id || "");
    hideList();
    setHint("Existing actor selected. Press Enter to open it.");
  }

  function renderSuggestions() {
    list.innerHTML = "";
    if (!suggestions.length) {
      hideList();
      return;
    }

    suggestions.forEach((actor, index) => {
      const item = document.createElement("button");
      item.type = "button";
      item.className = "actor-autocomplete-item" + (index === activeIndex ? " active" : "");
      item.setAttribute("role", "option");
      item.textContent = String(actor.display_name || "");
      item.addEventListener("mousedown", function (event) {
        event.preventDefault();
        applySuggestion(actor);
      });
      list.appendChild(item);
    });

    list.hidden = false;
  }

  function updateSuggestions() {
    const query = normalize(input.value);
    if (!query) {
      hideList();
      setHint("");
      return;
    }

    suggestions = actors
      .filter((actor) => normalize(actor.display_name).includes(query))
      .sort((a, b) => String(a.display_name || "").localeCompare(String(b.display_name || "")))
      .slice(0, 8);

    activeIndex = suggestions.length ? 0 : -1;
    renderSuggestions();

    const exact = exactMatchByName(input.value);
    if (exact) {
      selectedActorId = String(exact.id || "");
      setHint("Press Enter to open existing actor.");
    } else {
      selectedActorId = "";
      setHint(suggestions.length ? "Select an existing actor or continue to add a new one." : "No existing actor match. Add as new actor.");
    }
  }

  input.addEventListener("input", function () {
    selectedActorId = "";
    updateSuggestions();
  });

  input.addEventListener("keydown", function (event) {
    if (list.hidden || !suggestions.length) {
      if (event.key === "Escape") hideList();
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      activeIndex = (activeIndex + 1) % suggestions.length;
      renderSuggestions();
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      activeIndex = activeIndex <= 0 ? suggestions.length - 1 : activeIndex - 1;
      renderSuggestions();
      return;
    }

    if (event.key === "Enter" && activeIndex >= 0) {
      event.preventDefault();
      applySuggestion(suggestions[activeIndex]);
      return;
    }

    if (event.key === "Escape") {
      event.preventDefault();
      hideList();
    }
  });

  document.addEventListener("click", function (event) {
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.closest("#add-actor-form")) return;
    hideList();
  });

  form.addEventListener("submit", function (event) {
    const exact = selectedActorId ? actors.find((actor) => String(actor.id || "") === selectedActorId) : exactMatchByName(input.value);
    if (!exact) return;
    event.preventDefault();
    window.location.href = "/?actor_id=" + encodeURIComponent(String(exact.id || ""));
  });

  fetch("/actors", { headers: { Accept: "application/json" } })
    .then((response) => (response.ok ? response.json() : []))
    .then((items) => {
      actors = Array.isArray(items) ? items.filter((item) => item && item.id && item.display_name) : [];
    })
    .catch(() => {
      actors = [];
      setHint("Autocomplete unavailable. You can still add actors manually.");
    });
})();
