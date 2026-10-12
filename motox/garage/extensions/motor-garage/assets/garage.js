/* Motox-garage: gekozen motor in de header, cookie, en badge op de productpagina.
   Samenvoegen met het klantaccount gebeurt in modules/motox_garage/model.py. */
(function () {
  "use strict";

  var MAX = 20;
  var YEAR_MIN = 1950;
  var YEAR_MAX = 2040;
  var COOKIE = "motox_garage";
  var COPY = {
    stripEmpty: "Onderdelen voor jouw motor",
    stripSet: "Motor kiezen",
    stripSetMobile: "Kies jouw motor voor bijpassende onderdelen",
    stripFor: "Jouw geselecteerde motor:",
    stripForMobile: "Gekozen motor:",
    stripSwitch: "wijzigen",
    dialogTitle: "Voor welke motor zoek je?",
    dialogSub: "We onthouden hem op dit apparaat. Ben je ingelogd, dan staat hij ook bij je account.",
    make: "Merk",
    model: "Model",
    year: "Bouwjaar",
    pick: "Maak een keuze",
    save: "Deze motor gebruiken",
    close: "Sluiten",
    saved: "Jouw garage, kies een motorfiets om te activeren",
    remove: "Weghalen",
    empty: "Je hebt nog geen motor vastgezet.",
    fits: "Dit past de geselecteerde motor:",
    noFit: "Dit is voor een andere motor dan je",
    catalogMissing: "De motorenlijst is even niet beschikbaar.",
    limit: "Je kunt 20 motoren bewaren.",
    filterLabel: "Toon alleen onderdelen voor",
    filterEmpty: "Geen onderdelen voor je",
    filterUnavailable: "Het motorfilter is even niet beschikbaar.",
  };

  var state = { activeId: null, vehicles: [] };
  var catalog = [];
  var product = null;
  var copy = COPY;
  var proxyPath = "/apps/garage";
  var PROXY_WAIT_MS = 20000;
  var PROXY_RETRY_MS = 45000;
  var PROXY_DOWN_KEY = "mxg-proxy-retry";
  var catalogUrl = "";
  var loggedIn = false;
  var ready = true;
  var dialogOpen = false;

  var strip;
  var arrowsEl;
  var promptEl;
  var bikeWrap;
  var bikeName;
  var bikeYear;
  var actionBtn;
  var backdrop;
  var dialog;
  var badge;
  var makeSelect;
  var modelSelect;
  var yearSelect;
  var addButton;
  var collectionPage = 1;
  var seenVehicleId = "";
  var filterKey = "";
  var filterInflight = false;
  var pendingGrid = null;
  var filterGen = 0;
  var filterAbort = null;
  var filterStarted = 0;
  var originalStarted = 0;
  var gridWriting = false;
  var savedGridHtml = "";
  var savedGridFor = "";
  var savedCount = "";
  var originalFetch = false;
  var gridObserver = null;
  var formNote;
  var fitmentByHandle = {};

  function boot() {
    var cfg = readConfig();
    if (!cfg) return;
    copy = Object.assign({}, COPY, cfg.copy || {});
    proxyPath = cfg.proxyPath || "/apps/garage";
    catalogUrl = cfg.catalogUrl || "";
    loggedIn = !!cfg.loggedIn;
    product = cfg.product || null;
    ready = !loggedIn;
    state = coerce(readCookie());
    build();
    render();
    alignStrip();
    var stripMobile = isMobileStrip();
    window.addEventListener("resize", function () {
      var next = isMobileStrip();
      if (next !== stripMobile) {
        stripMobile = next;
        render();
        var toggle = document.getElementById("mxg-filter-toggle");
        if (toggle) placeToggle(toggle);
      }
      alignStrip();
    });
    var warmTimer = 0;
    document.addEventListener("mouseover", function (event) {
      var link = event.target && event.target.closest ? event.target.closest("a[href*='/collections/']") : null;
      if (!link) return;
      window.clearTimeout(warmTimer);
      warmTimer = window.setTimeout(function () {
        warmCollection(link);
      }, 80);
    });
    loadCatalog();
    watchCollectionGrid();
    if (loggedIn) syncFromAccount();
  }

  function readConfig() {
    var node = document.getElementById("motox-garage-config");
    if (!node) return null;
    try {
      return JSON.parse(node.textContent || "{}");
    } catch (err) {
      return null;
    }
  }

  function cleanToken(value) {
    var text = String(value == null ? "" : value)
      .trim()
      .toUpperCase()
      .replace(/\s+/g, " ");
    if (!text || text.length > 80) return "";
    return text;
  }

  function cleanYear(value) {
    var text = String(value == null ? "" : value).trim();
    if (!/^\d{4}$/.test(text)) return "";
    var year = Number(text);
    if (year < YEAR_MIN || year > YEAR_MAX) return "";
    return text;
  }

  function vehicleId(make, model, year) {
    return make + "::" + model + "::" + year;
  }

  function normalize(raw) {
    var garage = { activeId: null, vehicles: [] };
    if (!raw || typeof raw !== "object") return garage;
    var seen = {};
    var items = Array.isArray(raw.vehicles) ? raw.vehicles : [];
    for (var i = 0; i < items.length; i += 1) {
      var item = items[i] || {};
      var make = cleanToken(item.make);
      var model = cleanToken(item.model);
      var year = cleanYear(item.year);
      if (!make || !model || !year) continue;
      var id = vehicleId(make, model, year);
      if (seen[id]) continue;
      seen[id] = true;
      garage.vehicles.push({ id: id, make: make, model: model, year: year });
      if (garage.vehicles.length >= MAX) break;
    }
    if (typeof raw.activeId === "string" && seen[raw.activeId]) {
      garage.activeId = raw.activeId;
    }
    return garage;
  }

  function coerce(raw) {
    var garage = normalize(raw);
    if (!garage.activeId && garage.vehicles.length) {
      garage.activeId = garage.vehicles[garage.vehicles.length - 1].id;
    }
    return garage;
  }

  function activeVehicle() {
    for (var i = 0; i < state.vehicles.length; i += 1) {
      if (state.vehicles[i].id === state.activeId) return state.vehicles[i];
    }
    return null;
  }

  function label(vehicle) {
    return vehicle.make + " " + vehicle.model;
  }

  function readCookie() {
    var parts = document.cookie ? document.cookie.split("; ") : [];
    for (var i = 0; i < parts.length; i += 1) {
      if (parts[i].indexOf(COOKIE + "=") !== 0) continue;
      try {
        return JSON.parse(decodeURIComponent(parts[i].slice(COOKIE.length + 1)));
      } catch (err) {
        return null;
      }
    }
    return null;
  }

  function writeCookie(garage) {
    var secure = location.protocol === "https:" ? "; Secure" : "";
    document.cookie =
      COOKIE +
      "=" +
      encodeURIComponent(JSON.stringify(garage)) +
      "; Path=/; Max-Age=31536000; SameSite=Lax" +
      secure;
  }

  function lookup(mapping, key) {
    if (!mapping || typeof mapping !== "object") return null;
    if (Object.prototype.hasOwnProperty.call(mapping, key)) return mapping[key];
    var folded = key.toLowerCase();
    var keys = Object.keys(mapping);
    for (var i = 0; i < keys.length; i += 1) {
      if (String(keys[i]).toLowerCase() === folded) return mapping[keys[i]];
    }
    return null;
  }

  function asObject(value) {
    if (!value) return null;
    if (typeof value === "string") {
      try {
        return JSON.parse(value);
      } catch (err) {
        return null;
      }
    }
    if (typeof value === "object") return value;
    return null;
  }

  var EBIKE_MODELS = {
    "AUDI\nELECTRIC MOUNTAIN BIKE": 1,
    "BERRIA\nCAYMAN": 1,
    "BH\nATOMX": 1,
    "BULLS\nSONIC EVO": 1,
    "CANNONDALE\nTESORO NEO CARBON": 1,
    "CONWAY\nEWME": 1,
    "E-FRAMER\nEIGER": 1,
    "E-FRAMER\nGRIMSEL": 1,
    "E-FRAMER\nGSTAAD": 1,
    "FANTIC\nXMF 1.7 CARBON": 1,
    "FANTIC\nXTF 1.6 CARBON": 1,
    "FANTIC\nXXF 2.0": 1,
    "FULGUR\nMULA": 1,
    "FULGUR\nTORA": 1,
    "GAS GAS\nG ENDURO": 1,
    "HARLEY DAVIDSON\nSERIAL 1": 1,
    "HUSQVARNA\nHARD CROSS": 1,
    "HUSQVARNA\nMOUNTAIN CROSS": 1,
    "LAMBRETTA\nASSIANO": 1,
    "LAMBRETTA\nBARONA": 1,
    "LAMBRETTA\nBRERA": 1,
    "LAMBRETTA\nROSERIO": 1,
    "LAMBRETTA\nTALIEDO": 1,
    "MENDIZ\nEX41": 1,
    "MMR\nKAIZEN": 1,
    "MMR\nX-BOLT": 1,
    "SPECIALIZED\nTURBO KENEVO": 1,
    "SPECIALIZED\nTURBO LEVO": 1,
    "THOK\nTK01": 1,
    "WHISTLE\nB-RUSH": 1,
    "YAMAHA\nBOOSTER": 1,
    "YAMAHA\nMORO-07": 1
  };

  function isEbikeModel(make, model) {
    return EBIKE_MODELS[cleanToken(make) + "\n" + cleanToken(model)] === 1;
  }

  function withoutEbike(fitsOn) {
    var data = asObject(fitsOn);
    if (!data) return null;
    var out = {};
    var any = false;
    Object.keys(data).forEach(function (make) {
      var models = data[make];
      if (!models || typeof models !== "object") return;
      Object.keys(models).forEach(function (model) {
        if (isEbikeModel(make, model)) return;
        if (!out[make]) out[make] = {};
        out[make][model] = models[model];
        any = true;
      });
    });
    return any ? out : {};
  }

  function productIsEbikeOnly() {
    if (!product || !product.fitsOn) return false;
    var original = asObject(product.fitsOn);
    if (!original || !Object.keys(original).length) return false;
    var motor = withoutEbike(product.fitsOn);
    return !motor || !Object.keys(motor).length;
  }

  function suppressEbikeYmm() {
    if (!productIsEbikeOnly()) return;
    document.querySelectorAll(".product__ymm").forEach(function (node) {
      node.remove();
    });
  }

  function hideEbikeCards(handles) {
    if (!handles || !handles.length) return;
    var want = {};
    handles.forEach(function (handle) {
      want[String(handle)] = true;
    });
    document.querySelectorAll(".card__ymm").forEach(function (node) {
      var scope = node.closest("li, .card-wrapper") || document;
      var link = scope.querySelector("a[href*='/products/']");
      var href = link ? link.getAttribute("href") || "" : "";
      var handle = href.split("/products/")[1] || "";
      handle = handle.split("?")[0].split("#")[0].replace(/\/$/, "");
      try {
        handle = decodeURIComponent(handle);
      } catch (err) {
        return;
      }
      if (want[handle]) node.remove();
    });
  }

  function fitStatus(fitsOn, vehicle) {
    if (!vehicle) return "none";
    var data = withoutEbike(fitsOn);
    if (!data || !Object.keys(data).length) return "universal";
    var models = lookup(data, vehicle.make);
    if (!models || typeof models !== "object") return "no";
    var years = lookup(models, vehicle.model);
    if (!Array.isArray(years)) return "no";
    for (var i = 0; i < years.length; i += 1) {
      if (cleanYear(years[i]) === vehicle.year) return "yes";
    }
    return "no";
  }

  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  function build() {
    strip = el("div", "mxg-strip");
    var lead = el("div", "mxg-lead");
    promptEl = el("p", "mxg-prompt");
    arrowsEl = el("span", "mxg-arrows", "\u00bb");
    arrowsEl.setAttribute("aria-hidden", "true");
    arrowsEl.hidden = true;
    bikeWrap = el("p", "mxg-bike");
    bikeName = el("span", "mxg-bike-name");
    bikeYear = el("span", "mxg-bike-year");
    bikeWrap.appendChild(bikeName);
    bikeWrap.appendChild(bikeYear);
    lead.appendChild(promptEl);
    lead.appendChild(arrowsEl);
    lead.appendChild(bikeWrap);
    actionBtn = el("button", "mxg-action");
    actionBtn.type = "button";
    actionBtn.addEventListener("click", openDialog);
    lead.appendChild(actionBtn);
    strip.appendChild(lead);

    backdrop = el("div", "mxg-backdrop");
    backdrop.hidden = true;
    backdrop.addEventListener("click", function (event) {
      if (event.target === backdrop) closeDialog();
    });
    dialog = el("div", "mxg-dialog");
    dialog.setAttribute("role", "dialog");
    dialog.setAttribute("aria-modal", "true");
    backdrop.appendChild(dialog);
    buildDialog();

    badge = el("p", "mxg-fit");
    badge.hidden = true;

    mountStrip(strip);
    document.body.appendChild(backdrop);
    if (product) mountBadge(badge);
    document.addEventListener("keydown", onKeydown);
  }

  function field(labelText) {
    var wrap = el("label", "mxg-field");
    wrap.appendChild(el("span", null, labelText));
    var select = el("select");
    wrap.appendChild(select);
    return { wrap: wrap, select: select };
  }

  function buildDialog() {
    var head = el("div", "mxg-dialog-head");
    var close = el("button", "mxg-close", copy.close);
    close.type = "button";
    close.addEventListener("click", closeDialog);
    head.appendChild(el("h2", "mxg-dialog-title", copy.dialogTitle));
    head.appendChild(close);
    dialog.appendChild(head);
    dialog.appendChild(el("p", "mxg-dialog-sub", copy.dialogSub));

    var fields = el("div", "mxg-fields");
    var makeField = field(copy.make);
    var modelField = field(copy.model);
    var yearField = field(copy.year);
    makeSelect = makeField.select;
    modelSelect = modelField.select;
    yearSelect = yearField.select;
    makeSelect.addEventListener("change", onMake);
    modelSelect.addEventListener("change", onModel);
    yearSelect.addEventListener("change", updateAddButton);
    fields.appendChild(makeField.wrap);
    fields.appendChild(modelField.wrap);
    fields.appendChild(yearField.wrap);
    dialog.appendChild(fields);

    addButton = el("button", "mxg-save", copy.save);
    addButton.type = "button";
    addButton.disabled = true;
    addButton.addEventListener("click", onAdd);
    dialog.appendChild(addButton);
    formNote = el("p", "mxg-note");
    dialog.appendChild(formNote);
    dialog.appendChild(el("h3", "mxg-saved-title", copy.saved));
    var list = el("ul", "mxg-pills");
    list.id = "mxg-saved";
    dialog.appendChild(list);
    fillMakes();
  }

  function mountStrip(node) {
    var header = document.querySelector("header");
    if (header && header.parentNode) header.insertAdjacentElement("afterend", node);
    else document.body.insertBefore(node, document.body.firstChild);
  }

  function alignStrip() {
    if (!strip) return;
    var left = 24;
    var collectionTitle = document.querySelector(
      "h1.collection-hero__title, .collection-hero__title, .collection__title"
    );
    if (collectionTitle) {
      var edge = collectionTitle.getBoundingClientRect().left;
      if (edge > 8) left = Math.round(edge);
    } else {
      var page = document.querySelector("main .page-width, #MainContent .page-width");
      if (page) {
        var rect = page.getBoundingClientRect();
        var pad = parseFloat(window.getComputedStyle(page).paddingLeft) || 0;
        if (rect.width > 200) left = Math.max(16, Math.round(rect.left + pad));
      }
    }
    strip.style.paddingLeft = left + "px";
    strip.style.paddingRight = left + "px";
  }

  function mountBadge(node) {
    var anchor = document.querySelector(
      "[data-motox-pdp], product-form, form[action*='/cart/add']"
    );
    if (anchor) anchor.insertBefore(node, anchor.firstChild);
    else document.body.appendChild(node);
  }

  function collectionHandle() {
    var parts = location.pathname.split("/");
    var index = parts.indexOf("collections");
    if (index < 0 || !parts[index + 1]) return "";
    try {
      return decodeURIComponent(parts[index + 1]);
    } catch (err) {
      return "";
    }
  }

  function filterOffKey() {
    return "mxg-filter-off";
  }

  function filterIsOn() {
    try {
      var choice = sessionStorage.getItem("mxg-filter-user");
      if (choice === "off") return false;
      if (choice === "on") return true;
      return sessionStorage.getItem(filterOffKey()) !== "1";
    } catch (err) {
      return true;
    }
  }

  function setFilterOn(on) {
    try {
      sessionStorage.setItem("mxg-filter-user", on ? "on" : "off");
      if (on) sessionStorage.removeItem(filterOffKey());
      else sessionStorage.setItem(filterOffKey(), "1");
    } catch (err) {
      return;
    }
  }

  function watchCollectionGrid() {
    if (gridObserver || location.pathname.indexOf("/collections/") === -1) return;
    var refitTimer = 0;
    function scheduleRefit() {
      window.clearTimeout(refitTimer);
      refitTimer = window.setTimeout(function () {
        if (gridWriting || filterInflight) return;
        var handle = collectionHandle();
        var grid = document.getElementById("product-grid");
        if (!handle || !grid) return;
        if (grid.getAttribute("data-mxg") === "1") {
          if (!document.getElementById("mxg-filter-toggle") && activeVehicle() && filterIsOn()) {
            showCollectionToggle(activeVehicle());
          }
          return;
        }
        if (!activeVehicle() || !filterIsOn() || proxyDown()) return;
        if (fitmentByHandle[handle] === false) return;
        filterKey = "";
        applyCollectionFilter();
      }, 40);
    }
    gridObserver = new MutationObserver(function () {
      if (gridWriting) return;
      var grid = document.getElementById("product-grid");
      if (filterInflight && pendingGrid && grid !== pendingGrid) {
        abortFilter();
        filterKey = "";
        scheduleRefit();
        return;
      }
      if (!grid || grid.getAttribute("data-mxg") === "1") {
        if (grid && grid.getAttribute("data-mxg") === "1" && !document.getElementById("mxg-filter-toggle")) {
          scheduleRefit();
        }
        return;
      }
      scheduleRefit();
    });
    gridObserver.observe(document.documentElement, { childList: true, subtree: true });
    document.addEventListener("shopify:section:load", function () {
      abortFilter();
      filterKey = "";
      scheduleRefit();
    });
    window.addEventListener("pageshow", function (event) {
      if (event.persisted) reviveFilter();
      scheduleRefit();
    });
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState === "visible") reviveFilter();
    });
    if (document.prerendering) document.addEventListener("prerenderingchange", scheduleRefit);
  }

  function abortFilter() {
    filterGen += 1;
    filterInflight = false;
    filterStarted = 0;
    pendingGrid = null;
    if (filterAbort) {
      try {
        filterAbort.abort();
      } catch (err) {}
      filterAbort = null;
    }
  }

  function clearProxyDown() {
    try {
      sessionStorage.removeItem(PROXY_DOWN_KEY);
    } catch (err) {}
  }

  function onFilterToggle(on) {
    abortFilter();
    clearProxyDown();
    originalFetch = false;
    originalStarted = 0;
    setFilterOn(on);
    collectionPage = 1;
    filterKey = "";
    document.documentElement.classList.remove("mxg-hold");
    if (on) applyCollectionFilter();
    else restoreGrid();
  }

  function bindToggle(input) {
    if (!input || input.getAttribute("data-mxg-bound") === "1") return;
    input.setAttribute("data-mxg-bound", "1");
    input.setAttribute("form", "mxg-detached");
    input.addEventListener("pointerdown", function (event) {
      event.stopPropagation();
    });
    input.addEventListener(
      "input",
      function (event) {
        event.stopPropagation();
      },
      true
    );
    input.addEventListener("change", function (event) {
      event.stopPropagation();
      onFilterToggle(input.checked);
    });
  }

  function reviveFilter() {
    if (filterInflight && filterStarted && Date.now() - filterStarted > PROXY_WAIT_MS + 1500) {
      abortFilter();
    }
    if (originalFetch && originalStarted && Date.now() - originalStarted > PROXY_WAIT_MS + 1500) {
      originalFetch = false;
      originalStarted = 0;
    }
    var grid = document.getElementById("product-grid");
    var painted = grid && grid.getAttribute("data-mxg") === "1";
    if (
      document.documentElement.classList.contains("mxg-hold") &&
      (painted || !filterIsOn() || !filterInflight)
    ) {
      document.documentElement.classList.remove("mxg-hold");
    }
    var label = document.getElementById("mxg-filter-toggle");
    if (label && !label.isConnected) label = null;
    if (label) {
      bindToggle(label.querySelector("input"));
      var box = label.querySelector("input");
      if (box) box.checked = filterIsOn();
    } else if (activeVehicle() && collectionHandle()) {
      showCollectionToggle(activeVehicle());
    }
  }

  function placeToggle(label) {
    if (!label) return;
    var mobile = window.matchMedia("(max-width: 749px)").matches;
    var host = document.getElementById("ProductGridContainer");
    var form = document.querySelector(
      ".motox-collection-toolbar .facets-vertical-form, .ktm-collection-toolbar .facets-vertical-form, #FacetSortForm .facets-vertical-form, #FacetSortForm, .facets-container"
    );
    /* Mobiel: toolbar is .small-hide — zet vinkje boven de productgrid */
    if (mobile && host) {
      if (label.parentNode !== host || label !== host.firstChild) {
        host.insertBefore(label, host.firstChild);
      }
      return;
    }
    if (form) {
      if (label.parentNode !== form || label !== form.firstChild) {
        form.insertBefore(label, form.firstChild);
      }
      return;
    }
    if (host && host.parentNode) {
      host.parentNode.insertBefore(label, host);
    }
  }

  function ensureToggle() {
    var existing = document.getElementById("mxg-filter-toggle");
    if (existing && existing.getAttribute("data-mxg-owned") === "1") {
      placeToggle(existing);
      return existing;
    }
    if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
    var form = document.querySelector(
      ".motox-collection-toolbar .facets-vertical-form, .ktm-collection-toolbar .facets-vertical-form, #FacetSortForm .facets-vertical-form, #FacetSortForm, .facets-container"
    );
    var host = document.getElementById("ProductGridContainer");
    if (!form && !host) return null;
    var label = el("label", "mxg-filter-toggle");
    label.id = "mxg-filter-toggle";
    label.setAttribute("data-mxg-owned", "1");
    label.hidden = true;
    var input = el("input");
    input.type = "checkbox";
    input.checked = filterIsOn();
    label.addEventListener("click", function (event) {
      event.stopPropagation();
    });
    bindToggle(input);
    label.appendChild(input);
    var caption = el("span");
    caption.id = "mxg-filter-label";
    label.appendChild(caption);
    placeToggle(label);
    return label;
  }

  function syncToggleLabel(vehicle) {
    var caption = document.getElementById("mxg-filter-label");
    if (!caption) return;
    caption.textContent = copy.filterLabel + " " + label(vehicle) + " (" + vehicle.year + ")";
  }

  function hideCollectionToggle() {
    var parked = document.getElementById("mxg-filter-toggle");
    if (parked) parked.hidden = true;
  }

  function showCollectionToggle(vehicle) {
    var toggle = ensureToggle();
    if (!toggle || !vehicle) return;
    toggle.hidden = false;
    syncToggleLabel(vehicle);
    var input = toggle.querySelector("input");
    if (input) input.checked = filterIsOn();
  }

  function releaseUnfittedCollection(handle) {
    fitmentByHandle[handle] = false;
    hideCollectionToggle();
    document.documentElement.classList.remove("mxg-hold");
    var grid = document.getElementById("product-grid");
    if (grid && grid.getAttribute("data-mxg") === "1") restoreGrid();
  }

  function rememberGrid() {
    var container = document.getElementById("ProductGridContainer");
    var sig = location.pathname + location.search;
    if (!container || container.querySelector("[data-mxg='1']")) return;
    if (savedGridFor === sig && savedGridHtml) return;
    savedGridHtml = container.innerHTML;
    savedGridFor = sig;
    var count = document.getElementById("ProductCountDesktop");
    savedCount = count ? count.textContent : "";
  }

  function collectionSectionId() {
    var host = document.querySelector(".motox-collection-section, .ktm-collection-section");
    var section = host && host.closest("[id^='shopify-section-']");
    if (!section) {
      var grid = document.getElementById("product-grid");
      section = grid && grid.closest("[id^='shopify-section-']");
    }
    return section ? section.id.replace("shopify-section-", "") : "";
  }

  function fetchOriginalGrid() {
    if (originalFetch) return;
    var id = collectionSectionId();
    var container = document.getElementById("ProductGridContainer");
    if (!id || !container) {
      document.documentElement.classList.remove("mxg-hold");
      return;
    }
    originalFetch = true;
    originalStarted = Date.now();
    var ctrl = new AbortController();
    var timer = setTimeout(function () {
      ctrl.abort();
    }, PROXY_WAIT_MS);
    var query = location.search ? location.search.replace(/^\?/, "") + "&" : "";
    fetch(location.pathname + "?section_id=" + encodeURIComponent(id) + "&" + query, { signal: ctrl.signal })
      .then(function (response) {
        clearTimeout(timer);
        if (!response.ok) throw new Error("section");
        return response.text();
      })
      .then(function (html) {
        var doc = new DOMParser().parseFromString(html, "text/html");
        var fresh = doc.getElementById("ProductGridContainer");
        if (!fresh) return;
        savedGridHtml = fresh.innerHTML;
        savedGridFor = location.pathname + location.search;
        var count = doc.getElementById("ProductCountDesktop");
        if (count) savedCount = count.textContent;
        originalFetch = false;
        originalStarted = 0;
        if (!filterIsOn()) restoreGrid();
      })
      .catch(function () {
        clearTimeout(timer);
        originalFetch = false;
        originalStarted = 0;
        document.documentElement.classList.remove("mxg-hold");
      });
  }

  function restoreGrid() {
    var container = document.getElementById("ProductGridContainer");
    if (!container) return;
    if (!savedGridHtml) {
      fetchOriginalGrid();
      return;
    }
    gridWriting = true;
    container.innerHTML = savedGridHtml;
    gridWriting = false;
    if (savedCount) {
      document.querySelectorAll("[id='ProductCountDesktop'], [id='ProductCount']").forEach(function (node) {
        node.textContent = savedCount;
      });
    }
    filterKey = "";
    restoreFacetCounts();
    document.documentElement.classList.remove("mxg-hold");
  }

  function proxyDown() {
    try {
      var at = Number(sessionStorage.getItem(PROXY_DOWN_KEY) || "0");
      return at > 0 && Date.now() - at < PROXY_RETRY_MS;
    } catch (err) {
      return false;
    }
  }

  function markProxyDown() {
    try {
      sessionStorage.setItem(PROXY_DOWN_KEY, String(Date.now()));
    } catch (err) {}
    document.documentElement.classList.remove("mxg-hold");
  }

  function proxyFetch(url, options) {
    if (proxyDown()) return Promise.reject(new Error("proxy-down"));
    var ctrl = new AbortController();
    var timer = setTimeout(function () {
      ctrl.abort();
    }, PROXY_WAIT_MS);
    var parent = options && options.signal;
    if (parent) {
      if (parent.aborted) ctrl.abort();
      else parent.addEventListener("abort", function () {
        ctrl.abort();
      });
    }
    var opts = Object.assign({ credentials: "same-origin" }, options || {}, { signal: ctrl.signal });
    return fetch(url, opts).then(
      function (response) {
        clearTimeout(timer);
        if (!response.ok) {
          markProxyDown();
          throw new Error("proxy");
        }
        return response;
      },
      function (err) {
        clearTimeout(timer);
        if (err && err.name === "AbortError") throw err;
        markProxyDown();
        throw err;
      }
    );
  }

  function holdGrid() {
    if (proxyDown()) return;
    if (!activeVehicle() || !filterIsOn()) return;
    if (location.pathname.indexOf("/collections/") === -1) return;
    document.documentElement.classList.add("mxg-hold");
  }

  function applyCollectionFilter() {
    watchCollectionGrid();
    var handle = collectionHandle();
    if (!handle || !document.getElementById("product-grid")) return;
    rememberGrid();
    var vehicle = activeVehicle();
    if (!vehicle) {
      hideCollectionToggle();
      document.documentElement.classList.remove("mxg-hold");
      restoreGrid();
      return;
    }
    if (fitmentByHandle[handle] === false) {
      releaseUnfittedCollection(handle);
      return;
    }
    showCollectionToggle(vehicle);
    if (vehicle.id !== seenVehicleId) {
      seenVehicleId = vehicle.id;
      collectionPage = 1;
      filterKey = "";
    }
    if (proxyDown() || !filterIsOn()) {
      var currentGrid = document.getElementById("product-grid");
      if (!proxyDown() && currentGrid && currentGrid.getAttribute("data-mxg") === "1") restoreGrid();
      else document.documentElement.classList.remove("mxg-hold");
      if (proxyDown() || fitmentByHandle[handle] === true) return;
    }
    var key = vehicle.id + "|" + collectionPage + "|" + location.pathname + location.search;
    var grid = document.getElementById("product-grid");
    if (key === filterKey && grid && grid.getAttribute("data-mxg") === "1") {
      document.documentElement.classList.remove("mxg-hold");
      return;
    }
    var cached = readGridCache(key);
    if (cached) {
      showCachedCollection(handle, vehicle, key, cached);
      return;
    }
    loadFiltered(handle, vehicle, collectionPage, key);
  }

  function gridCacheName(key) {
    return "mxg-grid:" + key;
  }

  function readGridCache(key) {
    try {
      var saved = JSON.parse(sessionStorage.getItem(gridCacheName(key)) || "null");
      if (!saved || !saved.payload || !saved.payload.ok) return null;
      if (Date.now() - saved.at > 600000) return null;
      return saved.payload;
    } catch (err) {
      return null;
    }
  }

  function writeGridCache(key, payload) {
    try {
      var storageKey = gridCacheName(key);
      sessionStorage.setItem(storageKey, JSON.stringify({ at: Date.now(), payload: payload }));
      var names = [];
      for (var i = 0; i < sessionStorage.length; i += 1) {
        var name = sessionStorage.key(i);
        if (name && name.indexOf("mxg-grid:") === 0) names.push(name);
      }
      if (names.length <= 8) return;
      names.sort(function (a, b) {
        var left = JSON.parse(sessionStorage.getItem(a) || "{}").at || 0;
        var right = JSON.parse(sessionStorage.getItem(b) || "{}").at || 0;
        return left - right;
      });
      names.slice(0, names.length - 8).forEach(function (name) {
        sessionStorage.removeItem(name);
      });
    } catch (err) {
      return;
    }
  }

  function showCachedCollection(handle, vehicle, key, payload) {
    hideEbikeCards(payload.hideYmm);
    if (payload.hasFitment === false) {
      releaseUnfittedCollection(handle);
      return;
    }
    fitmentByHandle[handle] = true;
    showCollectionToggle(vehicle);
    if (!filterIsOn()) {
      document.documentElement.classList.remove("mxg-hold");
      return;
    }
    filterKey = key;
    paintGrid(payload, vehicle);
  }

  function warmCollection(link) {
    var vehicle = activeVehicle();
    if (!vehicle || !filterIsOn() || !link) return;
    var href = link.getAttribute("href") || "";
    var match = href.match(/\/collections\/([^/?#]+)/);
    if (!match) return;
    var handle = match[1];
    try {
      handle = decodeURIComponent(handle);
    } catch (err) {
      return;
    }
    if (handle === collectionHandle()) return;
    var path = "/collections/" + handle;
    var key = vehicle.id + "|1|" + path;
    if (proxyDown() || window.__mxgWarming || readGridCache(key) || window.__mxgWarm === key) return;
    window.__mxgWarm = key;
    window.__mxgWarming = true;
    var next = new URLSearchParams();
    next.set("handle", handle);
    next.set("make", vehicle.make);
    next.set("model", vehicle.model);
    next.set("year", vehicle.year);
    next.set("page", "1");
    proxyFetch(proxyPath + "/collection?" + next.toString())
      .then(function (response) {
        if (!response.ok) throw new Error("warm");
        return response.json();
      })
      .then(function (payload) {
        if (payload && payload.ok) writeGridCache(key, payload);
      })
      .catch(function () {
        if (window.__mxgWarm === key) window.__mxgWarm = "";
      })
      .finally(function () {
        window.__mxgWarming = false;
      });
  }

  function loadFiltered(handle, vehicle, page, key) {
    if (filterInflight) return;
    var gen = filterGen;
    filterInflight = true;
    filterStarted = Date.now();
    pendingGrid = document.getElementById("product-grid");
    var ctrl = new AbortController();
    filterAbort = ctrl;
    rememberGrid();
    holdGrid();
    var params = new URLSearchParams(location.search);
    var next = new URLSearchParams();
    next.set("handle", handle);
    next.set("make", vehicle.make);
    next.set("model", vehicle.model);
    next.set("year", vehicle.year);
    next.set("page", String(page));
    params.forEach(function (value, name) {
      if (name === "page") return;
      if (name === "sort_by" || name.indexOf("filter.") === 0) next.append(name, value);
    });
    var pending = window.__mxgPrefetch;
    var request;
    if (pending && pending.key === key && !ctrl.signal.aborted) {
      window.__mxgPrefetch = null;
      request = pending.promise;
    } else {
      request = proxyFetch(proxyPath + "/collection?" + next.toString(), { signal: ctrl.signal });
    }
    request
      .then(function (response) {
        if (gen !== filterGen) return null;
        if (!response.ok) throw new Error("collection");
        return response.json();
      })
      .then(function (payload) {
        if (gen !== filterGen || !payload) return;
        if (!payload.ok) throw new Error("collection");
        writeGridCache(key, payload);
        if (payload.hasFitment === false) {
          releaseUnfittedCollection(handle);
          return;
        }
        fitmentByHandle[handle] = true;
        var current = activeVehicle();
        if (!current || current.id !== vehicle.id) {
          document.documentElement.classList.remove("mxg-hold");
          return;
        }
        showCollectionToggle(current);
        if (!filterIsOn()) {
          document.documentElement.classList.remove("mxg-hold");
          return;
        }
        filterKey = key;
        paintGrid(payload, current);
      })
      .catch(function (err) {
        if (gen !== filterGen) return;
        if (err && err.name === "AbortError") return;
        document.documentElement.classList.remove("mxg-hold");
        var broken = document.getElementById("product-grid");
        if (broken && broken.getAttribute("data-mxg") === "1" && !broken.children.length) {
          broken.removeAttribute("data-mxg");
          restoreGrid();
        }
        // Toon het vinkje ook bij API-fout, zodat de klant het filter kan uitzetten.
        var current = activeVehicle();
        if (current) showCollectionToggle(current);
        if (filterIsOn()) showFilterError();
      })
      .finally(function () {
        if (gen !== filterGen) return;
        var startedOn = pendingGrid;
        filterInflight = false;
        filterStarted = 0;
        pendingGrid = null;
        if (filterAbort === ctrl) filterAbort = null;
        var grid = document.getElementById("product-grid");
        if (
          !proxyDown() &&
          activeVehicle() &&
          filterIsOn() &&
          grid &&
          startedOn &&
          grid !== startedOn &&
          grid.getAttribute("data-mxg") !== "1"
        ) {
          document.documentElement.classList.remove("mxg-hold");
          applyCollectionFilter();
        }
      });
  }

  function paintGrid(payload, vehicle) {
    var grid = document.getElementById("product-grid");
    var container = document.getElementById("ProductGridContainer");
    if (!grid || !container) return;
    if (grid.tagName !== "UL") return;
    gridWriting = true;
    try {
    /* Stylesheets staan in liquid-tegels; bij innerHTML-wissen verdwijnen
       voorraadkleuren/fonts. Verplaats ze naar <head> vóór het legen. */
    preserveGridStylesheets(grid);
    grid.setAttribute("data-mxg", "1");
    grid.innerHTML = "";
    var products = payload.products || [];
    if (!products.length) {
      var empty = el("li", "grid__item mxg-filter-empty");
      var msg = el("p", "mxg-filter-empty__text");
      msg.textContent =
        copy.filterEmpty + " " + label(vehicle) + " (" + vehicle.year + ") in deze collectie.";
      empty.appendChild(msg);
      var showAll = el("button", "mxg-show-all", "Toon alle producten in deze collectie");
      showAll.type = "button";
      showAll.addEventListener("click", function () {
        setFilterOn(false);
        var toggle = document.querySelector("#mxg-filter-toggle input");
        if (toggle) toggle.checked = false;
        restoreGrid();
      });
      empty.appendChild(showAll);
      grid.appendChild(empty);
    } else {
      products.forEach(function (item) {
        grid.appendChild(productCard(item));
      });
    }
    var oldPager = container.querySelector(".pagination-wrapper");
    if (oldPager) oldPager.remove();
    var collection = container.querySelector(".collection");
    if (payload.pages > 1 && collection) collection.appendChild(pager(payload));
    var countText = payload.total + (payload.total === 1 ? " product" : " producten");
    document.querySelectorAll("[id='ProductCountDesktop'], [id='ProductCount']").forEach(function (node) {
      node.textContent = countText;
    });
    var note = document.getElementById("mxg-filter-error");
    if (note) note.remove();
    applyFacetCounts(payload.facets);
    document.documentElement.classList.remove("mxg-hold");
    } finally {
      gridWriting = false;
    }
  }

  function vendorTone(name) {
    var key = String(name || "")
      .toLowerCase()
      .replace(/[\s\-,]+/g, "");
    if (key.indexOf("gasgas") !== -1) return "gasgas";
    if (key.indexOf("husqvarna") !== -1) return "husqvarna";
    if (key === "wp" || key.indexOf("wp") === 0) return "wp";
    if (key.indexOf("ktm") !== -1) return "ktm";
    return "other";
  }

  function preserveGridStylesheets(grid) {
    if (!grid || !document.head) return;
    var seen = {};
    document.querySelectorAll('head link[rel="stylesheet"]').forEach(function (link) {
      if (link.href) seen[link.href] = true;
    });
    grid.querySelectorAll('link[rel="stylesheet"]').forEach(function (link) {
      var href = link.href;
      if (!href || seen[href]) {
        link.remove();
        return;
      }
      seen[href] = true;
      document.head.appendChild(link);
    });
  }

  function setFacetDisabled(input, empty) {
    var label = input.closest("label");
    if (empty) {
      if (!input.disabled) input.setAttribute("data-mxg-disabled", "1");
      input.disabled = true;
      if (label && !label.classList.contains("disabled")) {
        label.classList.add("disabled");
        label.setAttribute("data-mxg-label-disabled", "1");
      }
      return;
    }
    if (input.getAttribute("data-mxg-disabled") === "1") {
      input.disabled = false;
      input.removeAttribute("data-mxg-disabled");
    }
    if (label && label.getAttribute("data-mxg-label-disabled") === "1") {
      label.classList.remove("disabled");
      label.removeAttribute("data-mxg-label-disabled");
    }
  }

  function applyFacetCounts(facets) {
    if (!facets) return;
    var inputs = document.querySelectorAll(
      "#FacetFiltersForm input[type='checkbox'], #FacetFiltersFormMobile input[type='checkbox']"
    );
    var matched = 0;
    var groups = {};
    inputs.forEach(function (input) {
      var bucket = facets[input.name];
      if (!bucket) return;
      matched += 1;
      // Ontbreekt in de motortelling: 0. Anders blijft het Shopify-totaal staan.
      var count = facetCount(bucket, input.value);
      var item = input.closest("li");
      var text = item && item.querySelector(".facet-checkbox__text");
      if (text) writeFacetCount(text, count);
      var empty = count === 0 && !input.checked;
      if (item) item.classList.toggle("mxg-facet-empty", empty);
      setFacetDisabled(input, empty);
      if (!groups[input.name]) groups[input.name] = [];
      groups[input.name].push({ item: item, empty: empty });
    });
    Object.keys(groups).forEach(function (name) {
      var entries = groups[name];
      var live = entries.some(function (entry) {
        return !entry.empty;
      });
      entries.forEach(function (entry) {
        if (!entry.item) return;
        var hide = live && entry.empty;
        entry.item.classList.toggle("mxg-facet-hidden", hide);
        if (!hide && entry.item.classList.contains("hidden")) {
          entry.item.setAttribute("data-mxg-unhid", "1");
          entry.item.classList.remove("hidden");
        }
      });
      var sample = entries[0] && entries[0].item;
      var details = sample && sample.closest("details");
      if (!details) return;
      var more = details.querySelector("show-more-button");
      if (!more) return;
      var collapsed = details.querySelector(".show-more-item.hidden:not(.mxg-facet-hidden)");
      if (!collapsed) {
        more.setAttribute("data-mxg-more", "1");
        more.hidden = true;
      } else if (more.getAttribute("data-mxg-more") === "1") {
        more.hidden = false;
        more.removeAttribute("data-mxg-more");
      }
    });
    if (!matched) {
      document.querySelectorAll(".mxg-facet-empty").forEach(function (item) {
        item.classList.remove("mxg-facet-empty");
      });
    }
    var price = facets.price;
    if (!price) return;
    var maxText = formatPrice(price.max).replace("\u20ac ", "");
    document.querySelectorAll("input[name='filter.v.price.lte']").forEach(function (input) {
      if (!input.hasAttribute("data-mxg-max")) {
        input.setAttribute("data-mxg-max", input.getAttribute("data-max") || "");
        input.setAttribute("data-mxg-placeholder", input.getAttribute("placeholder") || "");
      }
      input.setAttribute("data-max", maxText);
      if (!input.value) input.setAttribute("placeholder", maxText);
    });
  }

  function writeFacetCount(textEl, count) {
    if (!textEl.hasAttribute("data-mxg-original")) {
      textEl.setAttribute("data-mxg-original", textEl.innerHTML);
    }
    var label = textEl.querySelector(".facet-checkbox__text-label");
    if (!label) return;
    var node = label.nextSibling;
    var written = " (" + count + ")";
    if (node && node.nodeType === 3) node.nodeValue = written;
    else label.insertAdjacentText("afterend", written);
    var hidden = textEl.parentElement && textEl.parentElement.querySelector(".visually-hidden");
    if (!hidden) return;
    if (!hidden.hasAttribute("data-mxg-original")) {
      hidden.setAttribute("data-mxg-original", hidden.textContent);
    }
    hidden.textContent = label.textContent + " (" + count + ")";
  }

  function facetCount(bucket, value) {
    if (Object.prototype.hasOwnProperty.call(bucket, value)) return bucket[value];
    var folded = String(value).toLowerCase();
    var keys = Object.keys(bucket);
    for (var i = 0; i < keys.length; i += 1) {
      if (keys[i].toLowerCase() === folded) return bucket[keys[i]];
    }
    return 0;
  }

  function restoreFacetCounts() {
    document.querySelectorAll("[data-mxg-original]").forEach(function (node) {
      var original = node.getAttribute("data-mxg-original") || "";
      if (original.indexOf("<") !== -1) node.innerHTML = original;
      else node.textContent = original;
      node.removeAttribute("data-mxg-original");
    });
    document.querySelectorAll(".mxg-facet-empty, .mxg-facet-hidden").forEach(function (item) {
      item.classList.remove("mxg-facet-empty");
      item.classList.remove("mxg-facet-hidden");
    });
    document.querySelectorAll("[data-mxg-unhid]").forEach(function (item) {
      item.classList.add("hidden");
      item.removeAttribute("data-mxg-unhid");
    });
    document.querySelectorAll("[data-mxg-disabled]").forEach(function (input) {
      input.disabled = false;
      input.removeAttribute("data-mxg-disabled");
    });
    document.querySelectorAll("[data-mxg-label-disabled]").forEach(function (label) {
      label.classList.remove("disabled");
      label.removeAttribute("data-mxg-label-disabled");
    });
    document.querySelectorAll("show-more-button[data-mxg-more]").forEach(function (more) {
      more.hidden = false;
      more.removeAttribute("data-mxg-more");
    });
    document.querySelectorAll("input[name='filter.v.price.lte']").forEach(function (input) {
      if (!input.hasAttribute("data-mxg-max")) return;
      input.setAttribute("data-max", input.getAttribute("data-mxg-max") || "");
      input.setAttribute("placeholder", input.getAttribute("data-mxg-placeholder") || "");
      input.removeAttribute("data-mxg-max");
      input.removeAttribute("data-mxg-placeholder");
    });
  }

  function productHref(item) {
    var handle = collectionHandle();
    var slug = item.handle || "";
    if (!slug && item.url) {
      var parts = String(item.url).split("/products/");
      slug = parts.length > 1 ? parts[1].split("?")[0].split("/")[0] : "";
    }
    if (handle && slug) {
      try {
        slug = decodeURIComponent(slug);
      } catch (err) {}
      return "/collections/" + encodeURIComponent(handle) + "/products/" + encodeURIComponent(slug);
    }
    return item.url || "#";
  }

  function productCard(item) {
    var href = productHref(item);
    var li = el("li", "grid__item");
    var wrap = el("div", "card-wrapper product-card-wrapper underline-links-hover");
    var card = el("div", "card card--standard card--media");
    card.style.setProperty("--ratio-percent", "100%");
    var inner = el("div", "card__inner color-background-1 gradient ratio");
    inner.style.setProperty("--ratio-percent", "100%");
    if (item.image) {
      var media = el("div", "card__media");
      var frame = el("div", "media media--transparent media--hover-effect");
      var img = el("img", "motion-reduce");
      img.alt = item.title || "";
      img.loading = "lazy";
      img.width = 533;
      img.height = 533;
      img.src = cardImage(item.image);
      frame.appendChild(img);
      media.appendChild(frame);
      inner.appendChild(media);
    }
    var content = el("div", "card__content");
    var info = el("div", "card__information");
    var heading = el("h3", "card__heading h5");
    var titleLink = el("a", "full-unstyled-link", item.title || "");
    titleLink.href = href;
    heading.appendChild(titleLink);
    info.appendChild(heading);
    var details = el("div", "card-information");
    if (item.fits) details.appendChild(el("p", "card__ymm", item.fits));
    if (item.vendor) {
      details.appendChild(
        el(
          "div",
          "caption-with-letter-spacing light ktm-card-vendor ktm-card-vendor--" + vendorTone(item.vendor),
          item.vendor
        )
      );
    }
    if (item.price) {
      var price = el("div", "price");
      var priceBox = el("div", "price__container");
      var regular = el("div", "price__regular");
      regular.appendChild(el("span", "price-item price-item--regular", formatPrice(item.price)));
      priceBox.appendChild(regular);
      price.appendChild(priceBox);
      details.appendChild(price);
    }
    details.appendChild(
      el(
        "div",
        "ktm-stock-indicator " +
          (item.available ? "ktm-stock-indicator-available" : "ktm-stock-indicator-not-available"),
        item.available ? "Direct beschikbaar" : "Levertijd van toepassing"
      )
    );
    info.appendChild(details);
    content.appendChild(info);
    card.appendChild(inner);
    card.appendChild(content);
    wrap.appendChild(card);
    li.appendChild(wrap);
    return li;
  }

  function cardImage(src) {
    if (src.indexOf("cdn.shopify.com") === -1) return src;
    if (src.indexOf("width=") !== -1) return src;
    return src + (src.indexOf("?") === -1 ? "?" : "&") + "width=533";
  }

  function formatPrice(amount) {
    var value = Number(amount);
    if (!isFinite(value)) return "";
    return "\u20ac " + value.toFixed(2).replace(".", ",");
  }

  function pager(payload) {
    var wrap = el("div", "pagination-wrapper");
    var nav = el("nav", "pagination");
    nav.setAttribute("aria-label", "Paginering");
    var list = el("ul", "pagination__list list-unstyled");
    if (payload.page > 1) list.appendChild(pageLink("\u2039", payload.page - 1));
    var status = el("li");
    status.appendChild(
      el("span", "pagination__item pagination__item--current", payload.page + " / " + payload.pages)
    );
    list.appendChild(status);
    if (payload.page < payload.pages) list.appendChild(pageLink("\u203a", payload.page + 1));
    nav.appendChild(list);
    wrap.appendChild(nav);
    return wrap;
  }

  function pageLink(text, page) {
    var item = el("li");
    var link = el("a", "pagination__item link", text);
    link.href = "#product-grid";
    link.addEventListener("click", function (event) {
      event.preventDefault();
      collectionPage = page;
      filterKey = "";
      applyCollectionFilter();
      var grid = document.getElementById("product-grid");
      if (grid && grid.scrollIntoView) grid.scrollIntoView({ block: "start" });
    });
    item.appendChild(link);
    return item;
  }

  function showFilterError() {
    if (document.getElementById("mxg-filter-error")) return;
    var grid = document.getElementById("product-grid");
    if (!grid || !grid.parentNode) return;
    var note = el("p", "mxg-filter-error", copy.filterUnavailable);
    note.id = "mxg-filter-error";
    grid.parentNode.insertBefore(note, grid);
  }

  function isMobileStrip() {
    return window.matchMedia("(max-width: 749px)").matches;
  }

  function render() {
    var vehicle = activeVehicle();
    var mobile = isMobileStrip();
    strip.classList.toggle("is-set", !!vehicle);
    if (!vehicle) {
      promptEl.textContent = copy.stripEmpty;
      arrowsEl.hidden = true;
      bikeWrap.hidden = true;
      actionBtn.textContent = mobile
        ? copy.stripSetMobile || copy.stripSet
        : copy.stripSet;
      actionBtn.classList.remove("is-edit");
    } else {
      promptEl.textContent = mobile
        ? copy.stripForMobile || copy.stripFor
        : copy.stripFor;
      arrowsEl.hidden = true;
      bikeWrap.hidden = false;
      bikeYear.hidden = true;
      bikeName.textContent = label(vehicle) + " " + vehicle.year;
      actionBtn.textContent = copy.stripSwitch;
      actionBtn.classList.add("is-edit");
    }
    renderSaved();
    renderBadge();
    updateAddButton();
    applyCollectionFilter();
  }

  function renderSaved() {
    var list = document.getElementById("mxg-saved");
    if (!list) return;
    list.innerHTML = "";
    if (!state.vehicles.length) {
      list.appendChild(el("li", "mxg-saved-empty", copy.empty));
      return;
    }
    state.vehicles.forEach(function (vehicle) {
      var item = el("li", "mxg-pill");
      if (vehicle.id === state.activeId) item.classList.add("is-active");
      var pick = el("button", "mxg-pill-pick", label(vehicle) + " " + vehicle.year);
      pick.type = "button";
      pick.addEventListener("click", function () {
        setActive(vehicle.id);
        closeDialog();
      });
      var remove = el("button", "mxg-pill-x", "×");
      remove.type = "button";
      remove.setAttribute("aria-label", copy.remove + " " + label(vehicle));
      remove.addEventListener("click", function () {
        removeVehicle(vehicle.id);
      });
      item.appendChild(pick);
      item.appendChild(remove);
      list.appendChild(item);
    });
  }

  function renderBadge() {
    if (!badge) return;
    var vehicle = activeVehicle();
    var status = fitStatus(product && product.fitsOn, vehicle);
    if (status !== "yes" && status !== "no") {
      badge.hidden = true;
      badge.textContent = "";
      return;
    }
    var name = label(vehicle) + " (" + vehicle.year + ")";
    badge.hidden = false;
    badge.className = "mxg-fit " + (status === "yes" ? "mxg-fit--yes" : "mxg-fit--no");
    badge.textContent = (status === "yes" ? copy.fits : copy.noFit) + " " + name;
  }

  function fillMakes() {
    if (!makeSelect) return;
    var current = makeSelect.value;
    fillSelect(makeSelect, copy.pick, catalog.map(function (entry) { return entry.make; }));
    if (current && hasOption(makeSelect, current)) makeSelect.value = current;
    onMake();
  }

  function fillSelect(select, placeholder, values) {
    select.innerHTML = "";
    var first = el("option", null, placeholder);
    first.value = "";
    first.disabled = true;
    first.selected = true;
    select.appendChild(first);
    values.forEach(function (value) {
      var option = el("option", null, value);
      option.value = value;
      select.appendChild(option);
    });
  }

  function hasOption(select, value) {
    for (var i = 0; i < select.options.length; i += 1) {
      if (select.options[i].value === value) return true;
    }
    return false;
  }

  function modelsFor(make) {
    for (var i = 0; i < catalog.length; i += 1) {
      if (catalog[i].make === make) return catalog[i].models || [];
    }
    return [];
  }

  function onMake() {
    var models = modelsFor(makeSelect.value);
    fillSelect(
      modelSelect,
      copy.pick,
      models.map(function (entry) { return entry.model; })
    );
    modelSelect.disabled = !makeSelect.value;
    onModel();
  }

  function onModel() {
    var years = [];
    modelsFor(makeSelect.value).forEach(function (entry) {
      if (entry.model === modelSelect.value) years = entry.years || [];
    });
    fillSelect(yearSelect, copy.pick, years);
    yearSelect.disabled = !modelSelect.value;
    updateAddButton();
  }

  function updateAddButton() {
    if (!addButton) return;
    var full = state.vehicles.length >= MAX;
    addButton.disabled = full || !makeSelect.value || !modelSelect.value || !yearSelect.value;
    formNote.textContent = full ? copy.limit : catalog.length ? "" : copy.catalogMissing;
  }

  function openDialog() {
    if (!ready) return;
    dialogOpen = true;
    backdrop.hidden = false;
    document.body.classList.add("mxg-lock");
    var close = dialog.querySelector(".mxg-close");
    if (close) close.focus();
  }

  function closeDialog() {
    dialogOpen = false;
    backdrop.hidden = true;
    document.body.classList.remove("mxg-lock");
  }

  function onKeydown(event) {
    if (event.key === "Escape" && dialogOpen) closeDialog();
  }

  function commit(next) {
    state = coerce(next);
    writeCookie(state);
    render();
    if (!loggedIn) return;
    saveAccount();
  }

  function setActive(id) {
    commit({ activeId: id, vehicles: state.vehicles });
  }

  function removeVehicle(id) {
    var vehicles = state.vehicles.filter(function (vehicle) {
      return vehicle.id !== id;
    });
    var activeId = state.activeId === id ? null : state.activeId;
    commit({ activeId: activeId, vehicles: vehicles });
  }

  function onAdd() {
    if (!ready || addButton.disabled) return;
    var make = cleanToken(makeSelect.value);
    var model = cleanToken(modelSelect.value);
    var year = cleanYear(yearSelect.value);
    if (!make || !model || !year) return;
    var id = vehicleId(make, model, year);
    var vehicles = state.vehicles.slice();
    if (!vehicles.some(function (vehicle) { return vehicle.id === id; })) {
      if (vehicles.length >= MAX) return;
      vehicles.push({ id: id, make: make, model: model, year: year });
    }
    commit({ activeId: id, vehicles: vehicles });
    makeSelect.selectedIndex = 0;
    onMake();
    closeDialog();
  }

  function loadCatalog() {
    var url = catalogUrl || proxyPath + "/vehicles";
    fetch(url, { credentials: catalogUrl ? "omit" : "same-origin" })
      .then(function (response) {
        if (!response.ok) throw new Error("catalog");
        return response.json();
      })
      .then(function (data) {
        catalog = data && Array.isArray(data.makes) ? data.makes : [];
        fillMakes();
      })
      .catch(function () {
        catalog = [];
        fillMakes();
      });
  }

  function syncFromAccount() {
    postJson(proxyPath + "/session/merge", state)
      .then(function (garage) {
        if (garage) {
          state = coerce(garage);
          writeCookie(state);
          render();
        }
      })
      .finally(function () {
        ready = true;
      });
  }

  function saveAccount() {
    var snapshot = state;
    postJson(proxyPath + "/session", snapshot, "PUT").then(function (garage) {
      if (!garage) return;
      if (JSON.stringify(snapshot.vehicles) !== JSON.stringify(state.vehicles)) return;
      state = coerce(garage);
      writeCookie(state);
      render();
    });
  }

  function postJson(url, garage, method) {
    return fetch(url, {
      method: method || "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(garage),
    })
      .then(function (response) {
        if (!response.ok) return null;
        return response.json();
      })
      .then(function (payload) {
        return payload && payload.garage ? payload.garage : null;
      })
      .catch(function () {
        return null;
      });
  }

  window.MotoxGarage = {
    setProduct: function (next) {
      product = next;
      renderBadge();
    },
  };

  if (document.getElementById("motox-garage-config")) boot();
  else document.addEventListener("DOMContentLoaded", boot);
})();
