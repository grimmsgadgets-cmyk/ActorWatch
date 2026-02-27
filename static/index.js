      (function () {
        const actorRestoreKey = "tracker:lastActorId";
        const actorLinks = Array.from(document.querySelectorAll(".actor-link[href*='actor_id=']"));

        function actorIdFromHref(href) {
          const rawHref = String(href || "").trim();
          if (!rawHref) return "";
          try {
            const parsed = new URL(rawHref, window.location.origin);
            return String(parsed.searchParams.get("actor_id") || "").trim();
          } catch (_error) {
            return "";
          }
        }

        function restoreActorIfMissing() {
          const validActorIds = actorLinks
            .map((link) => actorIdFromHref(link.getAttribute("href")))
            .filter((value) => value);
          if (!validActorIds.length) return "";
          const validSet = new Set(validActorIds);
          const storedActorId = String(localStorage.getItem(actorRestoreKey) || "").trim();
          const fallbackActorId = validActorIds[0];
          const restoredActorId = validSet.has(storedActorId) ? storedActorId : fallbackActorId;
          if (!restoredActorId) return "";
          localStorage.setItem(actorRestoreKey, restoredActorId);
          const params = new URLSearchParams(window.location.search);
          params.set("actor_id", restoredActorId);
          const query = params.toString();
          const target = query ? "/?" + query : "/";
          window.location.replace(target);
          return restoredActorId;
        }

        const actorIdFromUrl = String(new URLSearchParams(window.location.search).get("actor_id") || "").trim();
        if (actorIdFromUrl) localStorage.setItem(actorRestoreKey, actorIdFromUrl);

        const actorId = String(document.body.dataset.actorId || actorIdFromUrl || "").trim();
        if (!actorId) {
          restoreActorIfMissing();
          return;
        }
        localStorage.setItem(actorRestoreKey, actorId);
        actorLinks.forEach((link) => {
          link.addEventListener("click", () => {
            const linkActorId = actorIdFromHref(link.getAttribute("href"));
            if (linkActorId) localStorage.setItem(actorRestoreKey, linkActorId);
          });
        });

        const liveIndicator = document.getElementById("live-indicator");
        const notebookHealthChip = document.getElementById("notebook-health-chip");
        const notebookHealthMessage = document.getElementById("notebook-health-message");
        const notebookHealthProgress = document.getElementById("notebook-health-progress");
        const reportNode = document.getElementById("recent-reports");
        const targetsNode = document.getElementById("recent-targets");
        const impactNode = document.getElementById("recent-impact");
        const refreshTimelineList = document.getElementById("refresh-timeline-list");
        const refreshTimelineStatus = document.getElementById("refresh-timeline-status");
        const refreshTimelineUpdated = document.getElementById("refresh-timeline-updated");
        const refreshEtaValue = document.getElementById("refresh-eta-value");
        const refreshCacheSaved = document.getElementById("refresh-cache-saved");
        const refreshQueueDepth = document.getElementById("refresh-queue-depth");
        const refreshActorForm = document.getElementById("refresh-actor-form");
        const refreshActorButton = document.getElementById("refresh-actor-button");
        const terminalGenerateNotesButton = document.getElementById("terminal-generate-notes");
        const terminalAddNoteButton = document.getElementById("terminal-add-note");
        const questionFeedbackButtons = Array.from(document.querySelectorAll(".question-feedback-btn"));
        const quickCheckDoNext = document.getElementById("quick-check-do-next");
        const quickCheckRows = Array.from(document.querySelectorAll("details[data-quick-check='1']"));
        const analystPackActorSelect = document.getElementById("analyst-pack-actor-select");
        const analystPackExportJsonLink = document.getElementById("analyst-pack-export-json-link");
        const analystPackExportPdfLink = document.getElementById("analyst-pack-export-pdf-link");
        const uiActivityStrip = document.getElementById("ui-activity-strip");
        const uiActivityText = document.getElementById("ui-activity-text");
        const uiToastStack = document.getElementById("ui-toast-stack");
        const sectionNextChecks = document.getElementById("section-nextchecks");
        const sectionHistory = document.getElementById("section-history-left");
        const workflowTourOpen = document.getElementById("workflow-tour-open");
        const workflowTourModal = document.getElementById("workflow-tour-modal");
        const workflowTourClose = document.getElementById("workflow-tour-close");
        const workflowTourDismiss = document.getElementById("workflow-tour-dismiss");
        const mainTabButtons = Array.from(document.querySelectorAll("[data-main-tab]"));
        const mainPanels = Array.from(document.querySelectorAll("[data-main-panel]"));
        const notesTabButtons = Array.from(document.querySelectorAll("[data-notes-tab]"));
        const notesPanels = Array.from(document.querySelectorAll("[data-notes-panel]"));
        const advTabButtons = Array.from(document.querySelectorAll("[data-adv-tab]"));
        const advPanels = Array.from(document.querySelectorAll("[data-adv-panel]"));
        const quickNoteModal = document.getElementById("quick-note-modal");
        const quickNoteClose = document.getElementById("quick-note-close");
        const quickNoteCancel = document.getElementById("quick-note-cancel");
        const quickNoteForm = document.getElementById("quick-note-form");
        const quickNoteAnalyst = document.getElementById("quick-note-analyst");
        const quickNoteConfidence = document.getElementById("quick-note-confidence");
        const quickNoteClaimType = document.getElementById("quick-note-claim-type");
        const quickNoteEvidenceFields = document.getElementById("quick-note-evidence-fields");
        const quickNoteCitationUrl = document.getElementById("quick-note-citation-url");
        const quickNoteObservedOn = document.getElementById("quick-note-observed-on");
        const quickNoteText = document.getElementById("quick-note-text");
        const quickNoteStatus = document.getElementById("quick-note-status");
        const quickNoteAnalystKey = "tracker:quickNoteAnalyst";
        let activeUiOps = 0;
        let activityClearTimer = 0;
        const workflowTourHideKey = "tracker:workflowTourHide";

        function showToast(type, message) {
          if (!uiToastStack || !message) return;
          const toast = document.createElement("div");
          toast.className = "ui-toast " + String(type || "info");
          toast.textContent = String(message || "");
          uiToastStack.appendChild(toast);
          window.setTimeout(() => {
            if (toast.parentNode) toast.parentNode.removeChild(toast);
          }, 2600);
        }

        function setActivity(state, message) {
          if (!uiActivityStrip || !uiActivityText) return;
          window.clearTimeout(activityClearTimer);
          uiActivityStrip.hidden = false;
          uiActivityStrip.className = "notice status-chip ui-activity-strip status-" + String(state || "idle");
          uiActivityText.textContent = String(message || "Idle");
        }

        function beginUiOp(message) {
          activeUiOps += 1;
          setActivity("running", message || "Working...");
        }

        function finishUiOp(message) {
          activeUiOps = Math.max(0, activeUiOps - 1);
          if (activeUiOps > 0) return;
          setActivity("ready", message || "Done.");
          activityClearTimer = window.setTimeout(() => {
            if (!uiActivityStrip || activeUiOps > 0) return;
            uiActivityStrip.hidden = true;
          }, 1600);
        }

        function failUiOp(message) {
          activeUiOps = Math.max(0, activeUiOps - 1);
          setActivity("error", message || "Action failed.");
        }

        function setRegionBusy(node, busy) {
          if (!node) return;
          if (busy) {
            node.classList.add("region-busy");
            node.setAttribute("aria-busy", "true");
          } else {
            node.classList.remove("region-busy");
            node.setAttribute("aria-busy", "false");
          }
        }

        function setButtonLoading(button, loading, loadingText) {
          if (!button) return;
          if (loading) {
            if (!button.dataset.originalLabel) button.dataset.originalLabel = String(button.textContent || "");
            button.textContent = String(loadingText || "Working...");
            button.disabled = true;
            button.classList.add("is-loading-btn");
          } else {
            button.textContent = String(button.dataset.originalLabel || button.textContent || "");
            button.disabled = false;
            button.classList.remove("is-loading-btn");
          }
        }

        async function fetchLiveState() {
          const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/ui/live", { headers: { "Accept": "application/json" } });
          if (!response.ok) return null;
          return response.json();
        }

        function applyLiveState(data) {
          if (!data) return;
          if (liveIndicator) {
            const state = String(data.notebook_status || "idle");
            liveIndicator.textContent = "Live: " + state;
          }
          if (notebookHealthChip && notebookHealthMessage) {
            const state = String(data.notebook_status || "idle").toLowerCase();
            let chipClass = "status-idle";
            let message = "Notebook status unknown.";
            if (state === "running") {
              chipClass = "status-running";
              message = String(data.notebook_message || "Refreshing notebook...");
            } else if (state === "ready") {
              chipClass = "status-ready";
              message = String(data.notebook_message || "Notebook is ready.");
            } else if (state === "error") {
              chipClass = "status-error";
              message = String(data.notebook_message || "Refresh failed.");
            } else if (state === "idle") {
              chipClass = "status-idle";
              message = String(data.notebook_message || "Notebook is idle.");
            }
            notebookHealthChip.className = "notice status-chip status-button " + chipClass;
            notebookHealthMessage.textContent = message;
            if (notebookHealthProgress) notebookHealthProgress.style.display = state === "running" ? "" : "none";
            if (refreshTimelineStatus) {
              if (state === "running") {
                refreshTimelineStatus.className = "refresh-activity-live running";
                refreshTimelineStatus.textContent = "Refresh is running now";
              } else if (state === "error") {
                refreshTimelineStatus.className = "refresh-activity-live error";
                refreshTimelineStatus.textContent = "Last refresh needs attention";
              } else {
                refreshTimelineStatus.className = "refresh-activity-live ready";
                refreshTimelineStatus.textContent = "Live updates active";
              }
            }
          }
          const recent = data.recent_change_summary || {};
          if (reportNode && recent.new_reports !== undefined) reportNode.textContent = String(recent.new_reports);
          if (targetsNode && recent.targets !== undefined) targetsNode.textContent = String(recent.targets);
          if (impactNode && recent.damage !== undefined) impactNode.textContent = String(recent.damage);
        }

        async function runLiveRefresh() {
          try {
            const data = await fetchLiveState();
            applyLiveState(data);
          } catch (error) {
            if (liveIndicator) liveIndicator.textContent = "Live sync unavailable";
          }
        }

        function setWorkflowTourOpen(open) {
          if (!workflowTourModal) return;
          if (open) {
            workflowTourModal.classList.add("open");
            workflowTourModal.setAttribute("aria-hidden", "false");
          } else {
            workflowTourModal.classList.remove("open");
            workflowTourModal.setAttribute("aria-hidden", "true");
          }
        }

        function setQuickNoteOpen(open) {
          if (!quickNoteModal) return;
          if (open) {
            quickNoteModal.classList.add("open");
            quickNoteModal.setAttribute("aria-hidden", "false");
            if (quickNoteStatus) quickNoteStatus.textContent = "";
            if (quickNoteAnalyst && !quickNoteAnalyst.value) {
              quickNoteAnalyst.value = String(localStorage.getItem(quickNoteAnalystKey) || "");
            }
            updateQuickNoteMode();
            if (quickNoteText) quickNoteText.focus();
          } else {
            quickNoteModal.classList.remove("open");
            quickNoteModal.setAttribute("aria-hidden", "true");
          }
        }

        function updateQuickNoteMode() {
          const claimType = String((quickNoteClaimType && quickNoteClaimType.value) || "assessment").toLowerCase();
          const evidenceMode = claimType === "evidence";
          if (quickNoteEvidenceFields) quickNoteEvidenceFields.hidden = !evidenceMode;
        }

        function setMainTab(tabKey) {
          const key = String(tabKey || "who").toLowerCase();
          mainTabButtons.forEach((button) => {
            const active = String(button.getAttribute("data-main-tab") || "").toLowerCase() === key;
            button.classList.toggle("active", active);
            button.setAttribute("aria-selected", active ? "true" : "false");
          });
          mainPanels.forEach((panel) => {
            const visible = String(panel.getAttribute("data-main-panel") || "").toLowerCase() === key;
            panel.classList.toggle("tab-panel-hidden", !visible);
          });
          try {
            localStorage.setItem("tracker:mainTab", key);
          } catch (_error) {
            // Ignore storage failures.
          }
        }

        function setAdvancedTab(tabKey) {
          const key = String(tabKey || "history").toLowerCase();
          advTabButtons.forEach((button) => {
            const active = String(button.getAttribute("data-adv-tab") || "").toLowerCase() === key;
            button.classList.toggle("active", active);
            button.setAttribute("aria-selected", active ? "true" : "false");
          });
          advPanels.forEach((panel) => {
            const visible = String(panel.getAttribute("data-adv-panel") || "").toLowerCase() === key;
            panel.classList.toggle("tab-panel-hidden", !visible);
          });
          try {
            localStorage.setItem("tracker:advancedTab", key);
          } catch (_error) {
            // Ignore storage failures.
          }
        }

        function setNotesTab(tabKey) {
          const key = String(tabKey || "capture").toLowerCase();
          notesTabButtons.forEach((button) => {
            const active = String(button.getAttribute("data-notes-tab") || "").toLowerCase() === key;
            button.classList.toggle("active", active);
            button.setAttribute("aria-selected", active ? "true" : "false");
          });
          notesPanels.forEach((panel) => {
            const visible = String(panel.getAttribute("data-notes-panel") || "").toLowerCase() === key;
            panel.classList.toggle("tab-panel-hidden", !visible);
          });
          try {
            localStorage.setItem("tracker:notesTab", key);
          } catch (_error) {
            // Ignore storage failures.
          }
        }

        function isoToLocalText(rawValue) {
          const raw = String(rawValue || "").trim();
          if (!raw) return "Unknown time";
          const value = raw.endsWith("Z") ? raw : raw + "Z";
          const dt = new Date(value);
          if (Number.isNaN(dt.getTime())) return raw.replace("T", " ").slice(0, 19) || "Unknown time";
          return dt.toLocaleString([], { year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
        }

        function durationText(durationMs) {
          const ms = Number(durationMs || 0);
          if (!Number.isFinite(ms) || ms <= 0) return "";
          const secs = Math.round(ms / 100) / 10;
          return secs >= 60 ? (Math.round(secs / 6) / 10) + "m" : secs + "s";
        }

        function secondsText(secondsValue) {
          const seconds = Number(secondsValue);
          if (!Number.isFinite(seconds) || seconds < 0) return "n/a";
          if (seconds < 60) return "~" + Math.round(seconds) + "s";
          return "~" + (Math.round(seconds / 6) / 10) + "m";
        }

        function triggerText(triggerType) {
          return String(triggerType || "").toLowerCase() === "auto_refresh" ? "Automatic background refresh" : "Manual refresh";
        }

        function runStateText(status) {
          const value = String(status || "").toLowerCase();
          if (value === "completed") return "Finished";
          if (value === "error") return "Needs attention";
          return "In progress";
        }

        function phaseLabelText(phase) {
          const key = String((phase && phase.phase_key) || "").toLowerCase();
          if (key.includes("discover")) return "Checking sources";
          if (key.includes("fetch") || key.includes("ingest") || key.includes("import")) return "Loading source updates";
          if (key.includes("question")) return "Updating quick checks";
          if (key.includes("timeline")) return "Updating timeline evidence";
          if (key.includes("synth") || key.includes("summary")) return "Writing AI summary";
          if (key.includes("review")) return "Reviewing new changes";
          if (key.includes("notebook") || key.includes("build")) return "Building notebook view";
          const fallback = String((phase && phase.phase_label) || key || "Refresh step").trim();
          return fallback || "Refresh step";
        }

        function phaseStateText(status) {
          const value = String(status || "").toLowerCase();
          if (value === "completed") return "done";
          if (value === "error") return "needs attention";
          return "in progress";
        }

        function renderRefreshTimeline(runs) {
          if (!refreshTimelineList) return;
          const entries = Array.isArray(runs) ? runs : [];
          refreshTimelineList.innerHTML = "";
          if (!entries.length) {
            const empty = document.createElement("div");
            empty.className = "refresh-empty";
            empty.textContent = "No recent refresh history yet for this actor.";
            refreshTimelineList.appendChild(empty);
            return;
          }
          entries.forEach((run) => {
            const card = document.createElement("div");
            const runStatus = String(run.status || "").toLowerCase();
            card.className = "refresh-run-card";
            if (runStatus === "running") card.classList.add("is-running");
            if (runStatus === "error") card.classList.add("is-error");

            const top = document.createElement("div");
            top.className = "refresh-run-top";
            const title = document.createElement("strong");
            title.textContent = runStateText(runStatus);
            const meta = document.createElement("span");
            const duration = durationText(run.duration_ms);
            meta.className = "refresh-run-meta";
            meta.textContent = isoToLocalText(run.created_at) + " | " + triggerText(run.trigger_type) + (duration ? " | " + duration : "");
            top.append(title, meta);
            card.appendChild(top);

            const message = document.createElement("div");
            message.className = "refresh-run-message";
            message.textContent = String(
              run.error_message
                || run.final_message
                || (runStatus === "running" ? "Refresh is still running." : "Refresh completed.")
            );
            card.appendChild(message);

            const phases = Array.isArray(run.phases) ? run.phases : [];
            if (phases.length) {
              const ul = document.createElement("ul");
              ul.className = "refresh-phase-list";
              phases.slice(0, 6).forEach((phase) => {
                const li = document.createElement("li");
                const label = document.createElement("strong");
                label.textContent = phaseLabelText(phase) + ": ";
                li.appendChild(label);
                const phaseMessage = String(phase.message || "").trim();
                li.appendChild(
                  document.createTextNode(
                    phaseStateText(phase.status) + (phaseMessage ? " - " + phaseMessage : "")
                  )
                );
                ul.appendChild(li);
              });
              card.appendChild(ul);
            }
            refreshTimelineList.appendChild(card);
          });
        }

        let refreshTimelinePollInFlight = false;
        async function runRefreshTimelinePoll() {
          if (!actorId || !refreshTimelineList || refreshTimelinePollInFlight) return;
          refreshTimelinePollInFlight = true;
          try {
            const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/refresh/timeline", {
              headers: { "Accept": "application/json" }
            });
            if (!response.ok) throw new Error("refresh timeline unavailable");
            const payload = await response.json();
            renderRefreshTimeline(payload.recent_generation_runs || []);
            if (refreshEtaValue) refreshEtaValue.textContent = secondsText(payload.eta_seconds);
            if (refreshCacheSaved) {
              const savedMs = Number(((payload.llm_cache_state || {}).saved_ms_total) || 0);
              refreshCacheSaved.textContent = savedMs > 0 ? ("~" + Math.round(savedMs / 1000) + "s") : "0s";
            }
            if (refreshQueueDepth) {
              const queue = payload.queue_state || {};
              const queued = Number(queue.generation_queued || 0);
              const running = Number(queue.generation_running || 0);
              refreshQueueDepth.textContent = String(queued + running) + " (" + String(running) + " running)";
            }
            if (refreshTimelineUpdated) refreshTimelineUpdated.textContent = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            if (refreshTimelineStatus) {
              refreshTimelineStatus.className = "refresh-activity-live ready";
              refreshTimelineStatus.textContent = "Live updates active";
            }
          } catch (_error) {
            if (refreshTimelineStatus) {
              refreshTimelineStatus.className = "refresh-activity-live error";
              refreshTimelineStatus.textContent = "Live updates temporarily unavailable";
            }
          } finally {
            refreshTimelinePollInFlight = false;
          }
        }

        async function submitRefreshJob() {
          if (!actorId) return;
          setButtonLoading(refreshActorButton, true, "Starting...");
          beginUiOp("Starting refresh...");
          if (refreshTimelineStatus) {
            refreshTimelineStatus.className = "refresh-activity-live running";
            refreshTimelineStatus.textContent = "Refresh is being queued";
          }
          try {
            const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/refresh/jobs", {
              method: "POST",
              headers: { "Accept": "application/json" }
            });
            if (!response.ok) throw new Error("refresh submit failed");
            const payload = await response.json();
            const queued = Boolean(payload.queued);
            showToast("success", queued ? "Refresh started." : "Refresh already in progress.");
            finishUiOp(queued ? "Refresh started." : "Refresh already running.");
            await runLiveRefresh();
            await runRefreshTimelinePoll();
          } catch (_error) {
            failUiOp("Refresh start failed.");
            showToast("error", "Could not start refresh.");
          } finally {
            setButtonLoading(refreshActorButton, false);
          }
        }

        setInterval(runLiveRefresh, 20000);
        runLiveRefresh();
        setInterval(runRefreshTimelinePoll, 20000);
        runRefreshTimelinePoll();

        function syncAnalystPackLinks() {
          const selectedActor = String((analystPackActorSelect && analystPackActorSelect.value) || actorId || "").trim();
          if (!selectedActor) return;
          if (analystPackExportJsonLink) {
            analystPackExportJsonLink.href = "/actors/" + encodeURIComponent(selectedActor) + "/export/analyst-pack.json";
          }
          if (analystPackExportPdfLink) {
            analystPackExportPdfLink.href = "/actors/" + encodeURIComponent(selectedActor) + "/export/analyst-pack.pdf";
          }
        }
        if (analystPackActorSelect) {
          analystPackActorSelect.addEventListener("change", syncAnalystPackLinks);
        }
        if (analystPackExportJsonLink || analystPackExportPdfLink) {
          syncAnalystPackLinks();
        }


        async function submitQuestionFeedback(threadId, feedbackValue, statusNode, triggerButton) {
          const payload = {
            item_type: "priority_question",
            item_id: String(threadId || ""),
            feedback: String(feedbackValue || "partial"),
            reason: ""
          };
          setButtonLoading(triggerButton, true, "Saving...");
          setRegionBusy(sectionNextChecks, true);
          beginUiOp("Saving quick-check feedback...");
          if (statusNode) statusNode.textContent = "Saving...";
          try {
            const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/feedback", {
              method: "POST",
              headers: { "Content-Type": "application/json", "Accept": "application/json" },
              body: JSON.stringify(payload)
            });
            if (!response.ok) {
              if (statusNode) statusNode.textContent = "Failed";
              failUiOp("Quick-check feedback failed.");
              showToast("error", "Quick-check feedback failed.");
              return;
            }
            if (statusNode) statusNode.textContent = "Recorded";
            finishUiOp("Quick-check feedback saved.");
            showToast("success", "Quick-check feedback saved.");
          } catch (error) {
            if (statusNode) statusNode.textContent = "Failed";
            failUiOp("Quick-check feedback failed.");
            showToast("error", "Quick-check feedback failed.");
          } finally {
            setButtonLoading(triggerButton, false);
            setRegionBusy(sectionNextChecks, false);
          }
        }

        const severitySelect = document.getElementById("timeline-severity");
        const categorySelect = document.getElementById("timeline-category");
        const searchInput = document.getElementById("timeline-search");
        const timelineRows = Array.from(document.querySelectorAll("#timeline-filter-body tr"));
        const timelineEmpty = document.getElementById("timeline-empty");
        const timelineReset = document.getElementById("timeline-reset");
        const timelineFilterChips = document.getElementById("timeline-filter-chips");
        const observationCards = Array.from(document.querySelectorAll("[data-observation-item-type][data-observation-item-key]"));
        const observationLedgerList = document.getElementById("observation-ledger-list");
        const observationLedgerEmpty = document.getElementById("observation-ledger-empty");
        const observationLedgerCount = document.getElementById("observation-ledger-count");
        const ledgerFilterChips = document.getElementById("ledger-filter-chips");
        const ledgerFilterAnalyst = document.getElementById("ledger-filter-analyst");
        const ledgerFilterConfidence = document.getElementById("ledger-filter-confidence");
        const ledgerFilterFrom = document.getElementById("ledger-filter-from");
        const ledgerFilterTo = document.getElementById("ledger-filter-to");
        const ledgerLimit = document.getElementById("ledger-limit");
        const ledgerApply = document.getElementById("ledger-apply");
        const ledgerClear = document.getElementById("ledger-clear");
        const ledgerExportJson = document.getElementById("ledger-export-json");
        const ledgerExportCsv = document.getElementById("ledger-export-csv");
        const sinceReviewDate = document.getElementById("since-review-date");
        const sinceReviewObservations = document.getElementById("since-review-observations");
        const sinceReviewSources = document.getElementById("since-review-sources");
        const sinceReviewReports = document.getElementById("since-review-reports");
        const markReviewed = document.getElementById("mark-reviewed");
        const clearReviewed = document.getElementById("clear-reviewed");
        const reviewMeta = document.getElementById("review-meta");
        const sourceListRows = Array.from(document.querySelectorAll(".recent-source-list li[data-source-date]"));
        const reviewStorageKey = "tracker:lastReview:" + actorId;

        function applyTimelineFilter() {
          const severity = (severitySelect && severitySelect.value || "").toLowerCase();
          const category = (categorySelect && categorySelect.value || "").toLowerCase();
          const query = (searchInput && searchInput.value || "").trim().toLowerCase();
          let visibleCount = 0;
          timelineRows.forEach((row) => {
            const rowSeverity = (row.dataset.severity || "").toLowerCase();
            const rowCategory = (row.dataset.category || "").toLowerCase();
            const rowSearch = (row.dataset.search || "").toLowerCase();
            const matchesSeverity = !severity || rowSeverity === severity;
            const matchesCategory = !category || rowCategory === category;
            const matchesQuery = !query || rowSearch.includes(query);
            const visible = matchesSeverity && matchesCategory && matchesQuery;
            row.style.display = visible ? "" : "none";
            if (visible) visibleCount += 1;
          });
          if (timelineEmpty) timelineEmpty.style.display = visibleCount === 0 ? "" : "none";
          renderTimelineChips();
        }

        function addChip(container, label, onRemove) {
          if (!container) return;
          const chip = document.createElement("span");
          chip.className = "filter-chip";
          chip.textContent = label + " ";
          const button = document.createElement("button");
          button.type = "button";
          button.textContent = "x";
          button.addEventListener("click", onRemove);
          chip.appendChild(button);
          container.appendChild(chip);
        }

        function renderTimelineChips() {
          if (!timelineFilterChips) return;
          timelineFilterChips.innerHTML = "";
          const severity = (severitySelect && severitySelect.value || "").trim();
          const category = (categorySelect && categorySelect.value || "").trim();
          const query = (searchInput && searchInput.value || "").trim();
          if (severity) addChip(timelineFilterChips, "Severity: " + severity, () => { if (severitySelect) severitySelect.value = ""; applyTimelineFilter(); });
          if (category) addChip(timelineFilterChips, "Category: " + category, () => { if (categorySelect) categorySelect.value = ""; applyTimelineFilter(); });
          if (query) addChip(timelineFilterChips, "Text: " + query, () => { if (searchInput) searchInput.value = ""; applyTimelineFilter(); });
        }

        if (timelineRows.length) {
          [severitySelect, categorySelect, searchInput].forEach((element) => {
            if (!element) return;
            element.addEventListener("input", applyTimelineFilter);
            element.addEventListener("change", applyTimelineFilter);
          });
          if (timelineReset) {
            timelineReset.addEventListener("click", () => {
              if (severitySelect) severitySelect.value = "";
              if (categorySelect) categorySelect.value = "";
              if (searchInput) searchInput.value = "";
              applyTimelineFilter();
            });
          }
        }

        const observationStore = new Map();
        const observationHistoryStore = new Map();
        function observationMapKey(itemType, itemKey) {
          return String(itemType || "") + "::" + String(itemKey || "");
        }

        function ratingLabel(item) {
          const sr = String(item.source_reliability || "").trim();
          const ic = String(item.information_credibility || "").trim();
          return (sr || ic) ? sr + ic : "n/a";
        }

        function ledgerFilters() {
          return {
            analyst: String((ledgerFilterAnalyst && ledgerFilterAnalyst.value) || "").trim().toLowerCase(),
            confidence: String((ledgerFilterConfidence && ledgerFilterConfidence.value) || "").trim().toLowerCase(),
            updated_from: String((ledgerFilterFrom && ledgerFilterFrom.value) || "").trim(),
            updated_to: String((ledgerFilterTo && ledgerFilterTo.value) || "").trim(),
            limit: Math.max(1, parseInt(String((ledgerLimit && ledgerLimit.value) || "12"), 10) || 12)
          };
        }

        function buildQueryString(filters) {
          const params = new URLSearchParams();
          if (filters.analyst) params.set("analyst", filters.analyst);
          if (filters.confidence) params.set("confidence", filters.confidence);
          if (filters.updated_from) params.set("updated_from", filters.updated_from);
          if (filters.updated_to) params.set("updated_to", filters.updated_to);
          const query = params.toString();
          return query ? "?" + query : "";
        }

        function updateLedgerExportLinks(filters) {
          const query = buildQueryString(filters);
          if (ledgerExportJson) ledgerExportJson.href = "/actors/" + encodeURIComponent(actorId) + "/observations/export.json" + query;
          if (ledgerExportCsv) ledgerExportCsv.href = "/actors/" + encodeURIComponent(actorId) + "/observations/export.csv" + query;
        }

        function renderLedgerChips(filters) {
          if (!ledgerFilterChips) return;
          ledgerFilterChips.innerHTML = "";
          if (filters.analyst) addChip(ledgerFilterChips, "Analyst: " + filters.analyst, () => { if (ledgerFilterAnalyst) ledgerFilterAnalyst.value = ""; renderObservationLedger(); });
          if (filters.confidence) addChip(ledgerFilterChips, "Confidence: " + filters.confidence, () => { if (ledgerFilterConfidence) ledgerFilterConfidence.value = ""; renderObservationLedger(); });
          if (filters.updated_from) addChip(ledgerFilterChips, "From: " + filters.updated_from, () => { if (ledgerFilterFrom) ledgerFilterFrom.value = ""; renderObservationLedger(); });
          if (filters.updated_to) addChip(ledgerFilterChips, "To: " + filters.updated_to, () => { if (ledgerFilterTo) ledgerFilterTo.value = ""; renderObservationLedger(); });
        }

        function applyObservationCard(card) {
          const itemType = card.dataset.observationItemType || "";
          const itemKey = card.dataset.observationItemKey || "";
          const data = observationStore.get(observationMapKey(itemType, itemKey)) || {};
          const analystField = card.querySelector(".observation-analyst");
          const confidenceField = card.querySelector(".observation-confidence");
          const sourceReliabilityField = card.querySelector(".observation-source-reliability");
          const infoCredibilityField = card.querySelector(".observation-information-credibility");
          const sourceRefField = card.querySelector(".observation-source-ref");
          const claimTypeField = card.querySelector(".observation-claim-type");
          const citationUrlField = card.querySelector(".observation-citation-url");
          const observedOnField = card.querySelector(".observation-observed-on");
          const noteField = card.querySelector(".observation-note");
          const metaNode = card.querySelector("[data-observation-meta]");
          const guidanceNode = card.querySelector("[data-observation-guidance]");

          if (analystField && data.updated_by) analystField.value = data.updated_by;
          if (confidenceField && data.confidence) confidenceField.value = data.confidence;
          if (sourceReliabilityField && data.source_reliability !== undefined) sourceReliabilityField.value = data.source_reliability;
          if (infoCredibilityField && data.information_credibility !== undefined) infoCredibilityField.value = data.information_credibility;
          if (sourceRefField && data.source_ref) sourceRefField.value = data.source_ref;
          if (claimTypeField) claimTypeField.value = String(data.claim_type || "assessment");
          if (citationUrlField) citationUrlField.value = String(data.citation_url || "");
          if (observedOnField) observedOnField.value = String(data.observed_on || "");
          if (noteField && data.note) noteField.value = data.note;
          if (metaNode) {
            const updatedAt = String(data.updated_at || "");
            const updatedBy = String(data.updated_by || "");
            const confidence = String(data.confidence || "moderate");
            const claimType = String(data.claim_type || "assessment");
            const ratingText = ratingLabel(data);
            const observedOn = String(data.observed_on || "");
            metaNode.textContent = updatedAt
              ? "Saved " + updatedAt + " by " + (updatedBy || "analyst") + " | " + claimType + " | " + confidence + " | rating " + ratingText + (observedOn ? " | observed " + observedOn : "")
              : "";
          }
          if (guidanceNode) {
            const guidanceItems = Array.isArray(data.quality_guidance) ? data.quality_guidance : [];
            guidanceNode.textContent = guidanceItems.length ? "Guidance: " + guidanceItems.join(" ") : "";
          }
        }

        function applyObservationHistoryCard(card) {
          const itemType = card.dataset.observationItemType || "";
          const itemKey = card.dataset.observationItemKey || "";
          const historyNode = card.querySelector("[data-observation-history]");
          const toggleButton = card.querySelector(".observation-history-toggle");
          if (!historyNode || !toggleButton) return;
          const isOpen = card.dataset.historyOpen === "true";
          toggleButton.textContent = isOpen ? "Hide history" : "View history";
          historyNode.style.display = isOpen ? "" : "none";
          if (!isOpen) return;
          const rows = observationHistoryStore.get(observationMapKey(itemType, itemKey)) || [];
          if (!rows.length) {
            historyNode.textContent = "No history entries yet.";
            return;
          }
          historyNode.innerHTML = "";
          rows.forEach((entry) => {
            const row = document.createElement("div");
            row.className = "observation-history-item";
            const head = document.createElement("div");
            head.className = "observation-history-head";
            head.textContent = String(entry.updated_at || "")
              + " | " + String(entry.updated_by || "analyst")
              + " | " + String(entry.claim_type || "assessment")
              + " | " + String(entry.confidence || "moderate");
            const body = document.createElement("div");
            body.textContent = String(entry.note || "(no note text)");
            row.append(head, body);
            const citation = String(entry.citation_url || "").trim();
            const observedOn = String(entry.observed_on || "").trim();
            if (citation || observedOn) {
              const meta = document.createElement("div");
              meta.className = "observation-history-head";
              meta.textContent = (observedOn ? ("Observed: " + observedOn) : "") + (citation ? ((observedOn ? " | " : "") + citation) : "");
              row.appendChild(meta);
            }
            historyNode.appendChild(row);
          });
        }

        async function loadObservationHistory(itemType, itemKey) {
          const key = observationMapKey(itemType, itemKey);
          try {
            const response = await fetch(
              "/actors/" + encodeURIComponent(actorId) + "/observations/" + encodeURIComponent(itemType) + "/" + encodeURIComponent(itemKey) + "/history?limit=25",
              { headers: { "Accept": "application/json" } }
            );
            if (!response.ok) return;
            const payload = await response.json();
            const items = Array.isArray(payload.items) ? payload.items : [];
            observationHistoryStore.set(key, items);
            observationCards
              .filter((candidate) => (candidate.dataset.observationItemType || "") === itemType && (candidate.dataset.observationItemKey || "") === itemKey)
              .forEach(applyObservationHistoryCard);
          } catch (error) {
            // Keep page usable even if history endpoint is unavailable.
          }
        }

        function renderObservationLedger() {
          if (!observationLedgerList || !observationLedgerEmpty) return;
          observationLedgerList.innerHTML = "";
          const filters = ledgerFilters();
          updateLedgerExportLinks(filters);
          renderLedgerChips(filters);
          const items = Array.from(observationStore.values())
            .filter((item) => {
              const by = String(item.updated_by || "").toLowerCase();
              const conf = String(item.confidence || "").toLowerCase();
              const updated = String(item.updated_at || "").slice(0, 10);
              if (filters.analyst && !by.includes(filters.analyst)) return false;
              if (filters.confidence && conf !== filters.confidence) return false;
              if (filters.updated_from && updated && updated < filters.updated_from) return false;
              if (filters.updated_to && updated && updated > filters.updated_to) return false;
              return true;
            })
            .sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));
          if (!items.length) {
            observationLedgerEmpty.style.display = "";
            if (observationLedgerCount) observationLedgerCount.textContent = "0 shown";
            return;
          }
          observationLedgerEmpty.style.display = "none";
          const limited = items.slice(0, filters.limit);
          if (observationLedgerCount) observationLedgerCount.textContent = String(limited.length) + " shown";
          limited.forEach((item) => {
            const wrap = document.createElement("div");
            wrap.className = "ledger-item";
            const head = document.createElement("div");
            head.className = "ledger-head";
            const by = document.createElement("span");
            by.textContent = "By " + String(item.updated_by || "analyst");
            const at = document.createElement("span");
            at.textContent = String(item.updated_at || "");
            const conf = document.createElement("span");
            conf.textContent = "Confidence: " + String(item.confidence || "moderate");
            const rating = document.createElement("span");
            rating.textContent = "Rating: " + ratingLabel(item);
            head.append(by, at, conf, rating);
            const body = document.createElement("div");
            body.className = "ledger-body";
            body.textContent = String(item.note || "(no note text)");
            wrap.append(head, body);
            const claimMeta = document.createElement("div");
            claimMeta.className = "ledger-link";
            claimMeta.textContent =
              "Claim: " + String(item.claim_type || "assessment")
              + (item.observed_on ? " | Observed: " + String(item.observed_on) : "");
            wrap.appendChild(claimMeta);
            if (String(item.citation_url || "").trim()) {
              const citationWrap = document.createElement("div");
              citationWrap.className = "ledger-link";
              const citationLink = document.createElement("a");
              citationLink.href = String(item.citation_url);
              citationLink.target = "_blank";
              citationLink.rel = "noreferrer";
              citationLink.textContent = "Citation";
              citationWrap.appendChild(citationLink);
              wrap.appendChild(citationWrap);
            }
            const sourceTitle = String(item.source_title || item.source_name || "");
            const sourceUrl = String(item.source_url || "");
            if (sourceTitle || sourceUrl) {
              const linkWrap = document.createElement("div");
              linkWrap.className = "ledger-link";
              const sourceText = document.createElement(sourceUrl ? "a" : "span");
              if (sourceUrl) {
                sourceText.href = sourceUrl;
                sourceText.target = "_blank";
                sourceText.rel = "noreferrer";
              }
              sourceText.textContent = sourceTitle || sourceUrl;
              linkWrap.appendChild(sourceText);
              wrap.appendChild(linkWrap);
            }
            observationLedgerList.appendChild(wrap);
          });
          renderSinceReview();
        }

        function parseIsoDate(value) {
          const raw = String(value || "").trim();
          if (!raw) return null;
          const normalized = raw.replace("Z", "+00:00");
          const dt = new Date(normalized);
          if (!Number.isNaN(dt.getTime())) return dt;
          if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
            const dt2 = new Date(raw + "T00:00:00Z");
            return Number.isNaN(dt2.getTime()) ? null : dt2;
          }
          return null;
        }

        function sourceDateCandidates() {
          return sourceListRows
            .map((node) => {
              const attr = String(node.getAttribute("data-source-date") || "").trim();
              const badge = node.querySelector(".badge");
              const badgeText = badge ? String(badge.textContent || "").trim() : "";
              return attr || badgeText;
            })
            .filter((v) => v);
        }

        function renderSinceReview() {
          const baselineRaw = localStorage.getItem(reviewStorageKey) || "";
          const baseline = parseIsoDate(baselineRaw);
          if (!baseline) {
            if (sinceReviewDate) sinceReviewDate.textContent = "Not set";
            if (sinceReviewObservations) sinceReviewObservations.textContent = "0";
            if (sinceReviewSources) sinceReviewSources.textContent = "0";
            if (sinceReviewReports && reportNode) sinceReviewReports.textContent = String(reportNode.textContent || "0");
            if (reviewMeta) reviewMeta.textContent = "Set a baseline date to track changes between review cycles.";
            return;
          }
          if (sinceReviewDate) sinceReviewDate.textContent = baseline.toISOString().slice(0, 10);
          const obsCount = Array.from(observationStore.values()).filter((item) => {
            const dt = parseIsoDate(item.updated_at);
            return dt && dt >= baseline;
          }).length;
          if (sinceReviewObservations) sinceReviewObservations.textContent = String(obsCount);
          const sourceCount = sourceDateCandidates().filter((value) => {
            const dt = parseIsoDate(value);
            return dt && dt >= baseline;
          }).length;
          if (sinceReviewSources) sinceReviewSources.textContent = String(sourceCount);
          if (sinceReviewReports && reportNode) sinceReviewReports.textContent = String(reportNode.textContent || "0");
          if (reviewMeta) reviewMeta.textContent = "Baseline stored locally for this actor on this browser.";
        }


        if (markReviewed) {
          markReviewed.addEventListener("click", () => {
            localStorage.setItem(reviewStorageKey, new Date().toISOString());
            renderSinceReview();
          });
        }
        if (clearReviewed) {
          clearReviewed.addEventListener("click", () => {
            localStorage.removeItem(reviewStorageKey);
            renderSinceReview();
          });
        }

        async function loadObservations() {
          setRegionBusy(sectionHistory, true);
          try {
            const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/observations?limit=500", { headers: { "Accept": "application/json" } });
            if (!response.ok) return;
            const payload = await response.json();
            const items = Array.isArray(payload.items) ? payload.items : [];
            items.forEach((item) => {
              observationStore.set(observationMapKey(item.item_type, item.item_key), item);
            });
            observationCards.forEach(applyObservationCard);
            renderObservationLedger();
          } catch (error) {
            // Keep page usable even if observation endpoint is unavailable.
          } finally {
            setRegionBusy(sectionHistory, false);
          }
        }

        observationCards.forEach((card) => {
          const saveButton = card.querySelector(".observation-save");
          const historyToggle = card.querySelector(".observation-history-toggle");
          if (!saveButton) return;
          if (historyToggle) {
            historyToggle.addEventListener("click", async () => {
              const itemType = card.dataset.observationItemType || "";
              const itemKey = card.dataset.observationItemKey || "";
              const currentlyOpen = card.dataset.historyOpen === "true";
              card.dataset.historyOpen = currentlyOpen ? "false" : "true";
              if (!currentlyOpen) {
                const historyNode = card.querySelector("[data-observation-history]");
                if (historyNode) historyNode.textContent = "Loading history...";
                setButtonLoading(historyToggle, true, "Loading...");
                setRegionBusy(sectionHistory, true);
                beginUiOp("Loading note history...");
                try {
                  await loadObservationHistory(itemType, itemKey);
                  finishUiOp("History loaded.");
                } catch (_error) {
                  failUiOp("History load failed.");
                } finally {
                  setButtonLoading(historyToggle, false);
                  setRegionBusy(sectionHistory, false);
                }
              }
              observationCards
                .filter((candidate) => (candidate.dataset.observationItemType || "") === itemType && (candidate.dataset.observationItemKey || "") === itemKey)
                .forEach(applyObservationHistoryCard);
            });
          }
          saveButton.addEventListener("click", async () => {
            const itemType = card.dataset.observationItemType || "";
            const itemKey = card.dataset.observationItemKey || "";
            const updatedBy = String((card.querySelector(".observation-analyst") || {}).value || "");
            const confidence = String((card.querySelector(".observation-confidence") || {}).value || "moderate");
            const sourceReliability = String((card.querySelector(".observation-source-reliability") || {}).value || "");
            const informationCredibility = String((card.querySelector(".observation-information-credibility") || {}).value || "");
            const claimType = String((card.querySelector(".observation-claim-type") || {}).value || "assessment");
            const citationUrl = String((card.querySelector(".observation-citation-url") || {}).value || "");
            const observedOn = String((card.querySelector(".observation-observed-on") || {}).value || "");
            const sourceRef = String((card.querySelector(".observation-source-ref") || {}).value || "");
            const note = String((card.querySelector(".observation-note") || {}).value || "");
            const payload = {
              updated_by: updatedBy,
              confidence: confidence,
              source_reliability: sourceReliability,
              information_credibility: informationCredibility,
              claim_type: claimType,
              citation_url: citationUrl,
              observed_on: observedOn,
              source_ref: sourceRef,
              note: note
            };
            try {
              setButtonLoading(saveButton, true, "Saving...");
              setRegionBusy(sectionHistory, true);
              beginUiOp("Saving analyst note...");
              const metaNode = card.querySelector("[data-observation-meta]");
              if (metaNode) metaNode.textContent = "Saving...";
              const response = await fetch(
                "/actors/" + encodeURIComponent(actorId) + "/observations/" + encodeURIComponent(itemType) + "/" + encodeURIComponent(itemKey),
                { method: "POST", headers: { "Content-Type": "application/json", "Accept": "application/json" }, body: JSON.stringify(payload) }
              );
              if (!response.ok) {
                const metaNode = card.querySelector("[data-observation-meta]");
                if (metaNode) metaNode.textContent = "Save failed. Retry.";
                failUiOp("Analyst note save failed.");
                showToast("error", "Analyst note save failed.");
                return;
              }
              const data = await response.json();
              observationStore.set(observationMapKey(itemType, itemKey), data);
              const currentHistory = observationHistoryStore.get(observationMapKey(itemType, itemKey)) || [];
              observationHistoryStore.set(
                observationMapKey(itemType, itemKey),
                [data, ...currentHistory].slice(0, 25)
              );
              observationCards
                .filter((candidate) => (candidate.dataset.observationItemType || "") === itemType && (candidate.dataset.observationItemKey || "") === itemKey)
                .forEach((candidate) => {
                  applyObservationCard(candidate);
                  applyObservationHistoryCard(candidate);
                });
              renderObservationLedger();
              finishUiOp("Analyst note saved.");
              showToast("success", "Analyst note saved.");
            } catch (error) {
              const metaNode = card.querySelector("[data-observation-meta]");
              if (metaNode) metaNode.textContent = "Save failed. Retry.";
              failUiOp("Analyst note save failed.");
              showToast("error", "Analyst note save failed.");
            } finally {
              setButtonLoading(saveButton, false);
              setRegionBusy(sectionHistory, false);
            }
          });
        });

        if (ledgerApply) ledgerApply.addEventListener("click", renderObservationLedger);
        if (ledgerClear) {
          ledgerClear.addEventListener("click", () => {
            if (ledgerFilterAnalyst) ledgerFilterAnalyst.value = "";
            if (ledgerFilterConfidence) ledgerFilterConfidence.value = "";
            if (ledgerFilterFrom) ledgerFilterFrom.value = "";
            if (ledgerFilterTo) ledgerFilterTo.value = "";
            if (ledgerLimit) ledgerLimit.value = "12";
            renderObservationLedger();
          });
        }

        questionFeedbackButtons.forEach((button) => {
          button.addEventListener("click", () => {
            const threadId = String(button.getAttribute("data-thread-id") || "");
            const feedbackValue = String(button.getAttribute("data-feedback") || "partial");
            const statusNode = document.getElementById("feedback-status-" + threadId);
            if (!threadId) return;
            submitQuestionFeedback(threadId, feedbackValue, statusNode, button);
          });
        });
        if (quickCheckDoNext && quickCheckRows.length) {
          quickCheckDoNext.addEventListener("click", () => {
            const sortedRows = quickCheckRows
              .slice()
              .filter((row) => row && row.offsetParent !== null)
              .sort((a, b) => {
                const rankA = Number.parseInt(String(a.dataset.quickCheckRank || "999"), 10);
                const rankB = Number.parseInt(String(b.dataset.quickCheckRank || "999"), 10);
                const safeA = Number.isFinite(rankA) ? rankA : 999;
                const safeB = Number.isFinite(rankB) ? rankB : 999;
                return safeA - safeB;
              });
            const nextRow = sortedRows[0];
            if (!nextRow) return;
            nextRow.open = true;
            nextRow.scrollIntoView({ behavior: "smooth", block: "start" });
          });
        }
        if (refreshActorForm) {
          refreshActorForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            await submitRefreshJob();
          });
        }

        if (terminalGenerateNotesButton) {
          terminalGenerateNotesButton.addEventListener("click", async () => {
            beginUiOp("Generating notes from current activity...");
            setButtonLoading(terminalGenerateNotesButton, true, "Generating...");
            try {
              const response = await fetch("/actors/" + encodeURIComponent(actorId) + "/observations/auto-snapshot", {
                method: "POST",
                headers: { "Accept": "text/html" }
              });
              if (!response.ok) throw new Error("auto snapshot failed");
              await loadObservations();
              finishUiOp("Notes generated.");
              showToast("success", "Generated notes from current activity.");
            } catch (_error) {
              failUiOp("Generate notes failed.");
              showToast("error", "Could not generate notes right now.");
            } finally {
              setButtonLoading(terminalGenerateNotesButton, false);
            }
          });
        }

        if (terminalAddNoteButton) {
          terminalAddNoteButton.addEventListener("click", () => setQuickNoteOpen(true));
        }
        if (quickNoteClose) quickNoteClose.addEventListener("click", () => setQuickNoteOpen(false));
        if (quickNoteCancel) quickNoteCancel.addEventListener("click", () => setQuickNoteOpen(false));
        if (quickNoteModal) {
          quickNoteModal.addEventListener("click", (event) => {
            if (event.target === quickNoteModal) setQuickNoteOpen(false);
          });
        }
        if (quickNoteForm) {
          if (quickNoteClaimType) {
            quickNoteClaimType.addEventListener("change", updateQuickNoteMode);
          }
          quickNoteForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const analyst = String((quickNoteAnalyst && quickNoteAnalyst.value) || "").trim();
            const confidence = String((quickNoteConfidence && quickNoteConfidence.value) || "moderate").trim().toLowerCase();
            const claimType = String((quickNoteClaimType && quickNoteClaimType.value) || "assessment").trim().toLowerCase();
            const citationUrl = String((quickNoteCitationUrl && quickNoteCitationUrl.value) || "").trim();
            const observedOn = String((quickNoteObservedOn && quickNoteObservedOn.value) || "").trim();
            const note = String((quickNoteText && quickNoteText.value) || "").trim();
            if (!analyst || !note) {
              if (quickNoteStatus) quickNoteStatus.textContent = "Analyst and note are required.";
              return;
            }
            if (claimType === "evidence" && (!citationUrl || !observedOn)) {
              if (quickNoteStatus) quickNoteStatus.textContent = "Citation URL and observed date are required for evidence-backed notes.";
              return;
            }
            localStorage.setItem(quickNoteAnalystKey, analyst);
            beginUiOp("Saving analyst note...");
            if (quickNoteStatus) quickNoteStatus.textContent = "Saving...";
            try {
              const response = await fetch(
                "/actors/" + encodeURIComponent(actorId) + "/observations/actor/summary",
                {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                  },
                  body: JSON.stringify({
                    updated_by: analyst,
                    confidence: confidence,
                    note: note,
                    source_ref: claimType === "evidence" ? "terminal-panel-evidence" : "terminal-panel",
                    claim_type: claimType,
                    citation_url: citationUrl,
                    observed_on: observedOn
                  })
                }
              );
              if (!response.ok) throw new Error("save note failed");
              await loadObservations();
              if (quickNoteText) quickNoteText.value = "";
              if (quickNoteCitationUrl) quickNoteCitationUrl.value = "";
              if (quickNoteObservedOn) quickNoteObservedOn.value = "";
              if (quickNoteClaimType) quickNoteClaimType.value = "assessment";
              updateQuickNoteMode();
              if (quickNoteStatus) quickNoteStatus.textContent = "Saved.";
              setQuickNoteOpen(false);
              finishUiOp("Analyst note saved.");
              showToast("success", "Analyst note saved.");
            } catch (_error) {
              if (quickNoteStatus) quickNoteStatus.textContent = "Save failed. Retry.";
              failUiOp("Analyst note save failed.");
              showToast("error", "Analyst note save failed.");
            }
          });
        }

        if (workflowTourOpen) {
          workflowTourOpen.addEventListener("click", () => setWorkflowTourOpen(true));
        }
        if (workflowTourClose) {
          workflowTourClose.addEventListener("click", () => setWorkflowTourOpen(false));
        }
        if (workflowTourDismiss) {
          workflowTourDismiss.addEventListener("click", () => {
            localStorage.setItem(workflowTourHideKey, "1");
            setWorkflowTourOpen(false);
          });
        }
        if (workflowTourModal) {
          workflowTourModal.addEventListener("click", (event) => {
            if (event.target === workflowTourModal) setWorkflowTourOpen(false);
          });
        }
        if (workflowTourModal && localStorage.getItem(workflowTourHideKey) !== "1") {
          window.setTimeout(() => setWorkflowTourOpen(true), 700);
        }

        if (mainTabButtons.length && mainPanels.length) {
          mainTabButtons.forEach((button) => {
            button.addEventListener("click", () => {
              const key = String(button.getAttribute("data-main-tab") || "who");
              setMainTab(key);
            });
          });
          const storedTab = String(localStorage.getItem("tracker:mainTab") || "who");
          setMainTab(storedTab);
        }
        if (advTabButtons.length && advPanels.length) {
          advTabButtons.forEach((button) => {
            button.addEventListener("click", () => {
              const key = String(button.getAttribute("data-adv-tab") || "history");
              setAdvancedTab(key);
            });
          });
          const storedAdvancedTab = String(localStorage.getItem("tracker:advancedTab") || "history");
          setAdvancedTab(storedAdvancedTab);
        }
        if (notesTabButtons.length && notesPanels.length) {
          notesTabButtons.forEach((button) => {
            button.addEventListener("click", () => {
              const key = String(button.getAttribute("data-notes-tab") || "capture");
              setNotesTab(key);
            });
          });
          const storedNotesTab = String(localStorage.getItem("tracker:notesTab") || "capture");
          setNotesTab(storedNotesTab);
        }

        loadObservations();
        if (timelineRows.length) renderTimelineChips();
        renderSinceReview();

      })();
