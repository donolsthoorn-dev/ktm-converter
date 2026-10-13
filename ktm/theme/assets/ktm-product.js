/* KTM productpagina: Motox-layout, BlenderPro alleen op titel/tabs */
(function () {
  if (document.documentElement.dataset.ktmProduct === "1") return;
  var root = document.querySelector("product-info");
  if (!root) return;
  document.documentElement.dataset.ktmProduct = "1";

  var check =
    '<svg viewBox="0 0 12 12" aria-hidden="true"><path fill="currentColor" d="M4.6 8.6 1.8 5.8l.9-.9 1.9 1.9 4.7-4.7.9.9z"/></svg>';
  var cart =
    '<svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M6 6V5a4 4 0 1 1 8 0v1h2.2a1 1 0 0 1 1 .9l.8 9A1 1 0 0 1 17 17H3a1 1 0 0 1-1-1.1l.8-9a1 1 0 0 1 1-.9H6zm2 0h4V5a2 2 0 1 0-4 0v1z"/></svg>';

  function esc(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function vendorHandle(name) {
    return name
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
  }

  var vendorEl = null;
  var vendorName = "";
  root.querySelectorAll(".product__text").forEach(function (el) {
    if (vendorEl) return;
    var text = el.textContent.trim();
    if (!text) return;
    if (
      el.classList.contains("subtitle") ||
      el.classList.contains("caption-with-letter-spacing") ||
      el.classList.contains("caption")
    ) {
      vendorEl = el;
    }
  });
  var share = root.querySelector("share-button");
  if (vendorEl) {
    vendorName = vendorEl.textContent.trim();
    var brandRow = document.createElement("div");
    brandRow.className = "ktm-brand-row";
    vendorEl.parentElement.insertBefore(brandRow, vendorEl);
    brandRow.appendChild(vendorEl);
    if (share) brandRow.appendChild(share);

    function applyLogo(img) {
      if (!img || !img.naturalWidth) return false;
      vendorEl.textContent = "";
      vendorEl.classList.add("ktm-vendor");
      vendorEl.appendChild(img);
      return true;
    }

    function tryLogo(ext, next) {
      var img = new Image();
      img.alt = vendorName;
      img.src = "/cdn/shop/files/merk-" + vendorHandle(vendorName) + "." + ext;
      function done() {
        if (applyLogo(img)) return;
        if (next) next();
      }
      if (img.complete) {
        done();
        return;
      }
      img.addEventListener("load", done);
      img.addEventListener("error", function () {
        if (next) next();
      });
    }

    tryLogo("png", function () {
      tryLogo("jpg", function () {
        tryLogo("webp");
      });
    });
  } else if (share) {
    var info = root.querySelector(".product__info-container") || root;
    var brandRowOnly = document.createElement("div");
    brandRowOnly.className = "ktm-brand-row";
    info.insertBefore(brandRowOnly, info.firstChild);
    brandRowOnly.appendChild(share);
  }

  root.querySelectorAll(".product__text").forEach(function (el) {
    if (!el.textContent.trim() && !el.querySelector("img")) el.hidden = true;
  });

  (function placeYmmSummary() {
    var title = root.querySelector(".product__title");
    if (!title) return;
    var ymm = root.querySelector(".product__ymm");
    if (!ymm || !ymm.textContent.trim()) return;
    if (ymm.previousElementSibling === title) return;
    title.insertAdjacentElement("afterend", ymm);
  })();

  (function placeBikeSubtitle() {
    var title = root.querySelector(".product__title");
    var text = (root.getAttribute("data-bike-subtitle") || "").trim();
    if (!title || !text) return;
    if (root.querySelector(".ktm-bike-subtitle")) return;
    var el = document.createElement("p");
    el.className = "ktm-bike-subtitle";
    el.textContent = text;
    var after = title.nextElementSibling;
    if (after && after.classList.contains("product__ymm")) {
      after.insertAdjacentElement("afterend", el);
    } else {
      title.insertAdjacentElement("afterend", el);
    }
  })();

  var price = root.querySelector(".price");
  var tax = root.querySelector(".product__tax");
  if (price && price.parentElement) {
    var priceRow = document.createElement("div");
    priceRow.className = "ktm-price-row";
    price.parentElement.insertBefore(priceRow, price);
    var priceCol = document.createElement("div");
    priceRow.appendChild(priceCol);
    priceCol.appendChild(price);
    if (tax) {
      tax.textContent = "Inclusief BTW";
      priceCol.appendChild(tax);
    }

    var submit = root.querySelector(".product-form__submit");
    var soldOut = !!(
      submit &&
      (submit.hasAttribute("disabled") || submit.getAttribute("aria-disabled") === "true")
    );
    var etaBox = root.querySelector(".product-variant-inventory-policy-eta-date");
    var etaText = etaBox ? (etaBox.querySelector(".eta-date") || {}).textContent || "" : "";
    etaText = etaText.trim();
    var etaOn = !!(etaBox && getComputedStyle(etaBox).display !== "none" && etaText);
    if (etaOn || !soldOut) {
      var stock = document.createElement("div");
      stock.className = "ktm-stock " + (etaOn ? "ktm-stock--eta" : "ktm-stock--in");
      stock.innerHTML =
        '<span class="ktm-stock__label"><span class="ktm-stock__mark">' +
        check +
        "</span>" +
        esc(etaOn ? "Levertijd van toepassing" : "Op voorraad") +
        "</span>" +
        (etaOn
          ? '<span class="ktm-stock__detail">Verwacht vanaf ' + esc(etaText) + "</span>"
          : '<span class="ktm-stock__detail">Snel beschikbaar</span>');
      priceRow.appendChild(stock);
    }
    if (etaBox) etaBox.hidden = true;
  }

  var stylingSubmit = false;
  function styleSubmit() {
    if (stylingSubmit) return;
    var submit = root.querySelector(".product-form__submit");
    var buttons = root.querySelector(".product-form__buttons");
    var quantity = root.querySelector(".product-form__quantity");
    if (buttons && quantity && quantity.parentElement !== buttons) {
      buttons.insertBefore(quantity, buttons.firstChild);
    }
    if (!submit) return;
    var label = submit.querySelector(":scope > span");
    if (!label) return;
    var disabled = submit.hasAttribute("disabled") || submit.getAttribute("aria-disabled") === "true";
    var atc = "In winkelwagen";
    var ready =
      label.querySelector("svg") && label.textContent.toLowerCase().indexOf(atc.toLowerCase()) !== -1;
    if (disabled || ready) return;
    stylingSubmit = true;
    label.classList.add("ktm-atc");
    label.innerHTML = cart + " " + esc(atc);
    stylingSubmit = false;
  }
  styleSubmit();
  var submitBtn = root.querySelector(".product-form__submit");
  if (submitBtn) {
    new MutationObserver(styleSubmit).observe(submitBtn, {
      childList: true,
      subtree: true,
      characterData: true,
    });
  }

  var buttonsHost = root.querySelector(".product-form__buttons");
  if (buttonsHost && !root.querySelector(".ktm-buy-meta")) {
    var meta = document.createElement("div");
    meta.className = "ktm-buy-meta";
    var ids = document.createElement("div");
    ids.className = "ktm-buy-meta__ids";
    var sku = root.querySelector(".product__sku");
    if (sku) ids.appendChild(sku);
    var hasArticleTable = sku && sku.querySelector(".ktm-article-table");
    if (sku && vendorName && !hasArticleTable) {
      var sep = document.createElement("span");
      sep.className = "ktm-buy-meta__sep";
      sep.setAttribute("aria-hidden", "true");
      sep.textContent = "·";
      ids.appendChild(sep);
    }
    if (vendorName && !hasArticleTable) {
      var brand = document.createElement("span");
      brand.className = "ktm-meta__brand";
      brand.innerHTML = '<span class="product__label">Merk:</span> ' + esc(vendorName);
      ids.appendChild(brand);
    }
    meta.appendChild(ids);
    buttonsHost.insertAdjacentElement("afterend", meta);
  }

  var trustHost = root.querySelector(".ktm-buy-meta") || root.querySelector(".product-form__buttons");
  if (trustHost && !root.querySelector(".ktm-trust")) {
    var trust = document.createElement("div");
    trust.className = "ktm-trust";
    trust.innerHTML =
      '<div class="ktm-trust__item"><span class="ktm-trust__icon" aria-hidden="true"><svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 8h16v12H3z"/><path d="M19 12h6l4 4v4h-10"/><circle cx="9" cy="22" r="2.2"/><circle cx="23" cy="22" r="2.2"/></svg></span><span><span class="ktm-trust__title">Snelle levering.</span> <span class="ktm-trust__text">Snel beschikbaar.</span></span></div> ' +
      '<div class="ktm-trust__item"><span class="ktm-trust__icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg></span><span><span class="ktm-trust__title">Motorsport experts.</span> <span class="ktm-trust__text">30+ jaar ervaring.</span></span></div> ' +
      '<div class="ktm-trust__item"><span class="ktm-trust__icon" aria-hidden="true"><svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 12a8 8 0 1 1 2.2 6.5"/><path d="M8 8v4h4"/></svg></span><span><span class="ktm-trust__title">Eenvoudig retourneren.</span> <span class="ktm-trust__text">30 dagen retour.</span></span></div>';
    trustHost.insertAdjacentElement("afterend", trust);
  }

  (function setupSideGallery() {
    var gallery = document.querySelector("media-gallery");
    if (!gallery || gallery.dataset.ktmGallery === "1") return;
    gallery.dataset.ktmGallery = "1";

    var thumbs = gallery.querySelector(".thumbnail-slider");
    var list = thumbs ? thumbs.querySelector(".thumbnail-list") : null;
    var items = list ? Array.prototype.slice.call(list.querySelectorAll(".thumbnail-list__item")) : [];
    var visible = items.filter(function (item) {
      return getComputedStyle(item).display !== "none";
    });

    if (!thumbs || !list || visible.length <= 1) {
      gallery.classList.add("ktm-gallery--solo");
      if (thumbs) thumbs.hidden = true;
      return;
    }

    gallery.classList.add("ktm-gallery--side");
    thumbs.querySelectorAll(".slider-button").forEach(function (button) {
      button.hidden = true;
    });

    if (visible.length > 4) {
      thumbs.classList.add("ktm-thumbs--scroll");
      var nav = document.createElement("div");
      nav.className = "ktm-thumbs-nav";
      nav.innerHTML =
        '<button type="button" class="ktm-thumbs-nav__btn" data-dir="-1" aria-label="Vorige foto\'s"><svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M10 7.2 4.6 12.6l1.4 1.4L10 10l3.9 4 1.5-1.4z"/></svg></button>' +
        '<button type="button" class="ktm-thumbs-nav__btn" data-dir="1" aria-label="Volgende foto\'s"><svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M10 12.8 15.4 7.4 14 6l-4 4-3.9-4L4.6 7.4z"/></svg></button>';
      thumbs.appendChild(nav);

      var up = nav.querySelector('[data-dir="-1"]');
      var down = nav.querySelector('[data-dir="1"]');

      function stepSize() {
        var item = list.querySelector(".thumbnail-list__item");
        if (!item) return list.clientHeight;
        var styles = getComputedStyle(list);
        var gap = parseFloat(styles.rowGap || styles.gap) || 0;
        return item.getBoundingClientRect().height + gap;
      }

      function updateNav() {
        var max = Math.max(0, list.scrollHeight - list.clientHeight);
        up.disabled = list.scrollTop <= 2;
        down.disabled = list.scrollTop >= max - 2;
      }

      function pageScroll(dir) {
        list.scrollBy({ top: dir * stepSize() * 4, behavior: "smooth" });
        window.setTimeout(updateNav, 320);
      }

      up.addEventListener("click", function () {
        pageScroll(-1);
      });
      down.addEventListener("click", function () {
        pageScroll(1);
      });
      list.addEventListener("scroll", updateNav, { passive: true });
      list.scrollTop = 0;
      updateNav();
    }
  })();

  function yearRange(years) {
    if (!years || !years.length) return "";
    var nums = years
      .map(function (y) {
        return parseInt(String(y), 10);
      })
      .filter(function (n) {
        return !isNaN(n);
      })
      .sort(function (a, b) {
        return a - b;
      });
    if (!nums.length) return years.join(", ");
    var parts = [];
    var start = nums[0];
    var prev = nums[0];
    for (var i = 1; i <= nums.length; i++) {
      var cur = nums[i];
      if (cur === prev + 1) {
        prev = cur;
        continue;
      }
      parts.push(start === prev ? String(start) : start + "-" + prev);
      start = cur;
      prev = cur;
    }
    return parts.join(", ");
  }

  function modelSortKey(model) {
    var match = String(model || "").match(/^(\d+)/);
    return [match ? parseInt(match[1], 10) : 9999, String(model || "")];
  }

  function compareModels(a, b) {
    var left = modelSortKey(a);
    var right = modelSortKey(b);
    if (left[0] !== right[0]) return left[0] - right[0];
    return left[1].localeCompare(right[1]);
  }

  function buildFitsTable() {
    var dataEl = document.getElementById("ktm-fits-on-data");
    var box = document.getElementById("ktm-fits-on");
    if (!dataEl || !box) return null;
    var data;
    try {
      data = JSON.parse(dataEl.textContent || "null");
    } catch (e) {
      return null;
    }
    /* Oude producten hebben fits_on als plain string; Liquid | json
       encodeert die dan dubbel. Eén parse extra geeft het object. */
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch (e2) {
        return null;
      }
    }
    if (!data || typeof data !== "object") return null;
    var tbody = box.querySelector("tbody");
    if (!tbody) return null;
    tbody.innerHTML = "";
    var rows = 0;
    Object.keys(data)
      .sort()
      .forEach(function (make) {
        var models = data[make];
        if (!models || typeof models !== "object") return;
        Object.keys(models)
          .sort(compareModels)
          .forEach(function (model) {
            var years = models[model];
            var tr = document.createElement("tr");
            tr.innerHTML =
              "<td>" +
              esc(make) +
              "</td><td>" +
              esc(model) +
              "</td><td>" +
              esc(yearRange(Array.isArray(years) ? years : [])) +
              "</td>";
            tbody.appendChild(tr);
            rows += 1;
          });
      });
    if (!rows) return null;
    box.hidden = false;
    return box;
  }

  (function buildTabs() {
    var description = root.querySelector(".product__description");
    if (!description) {
      description = document.createElement("div");
      description.className = "product__description rte";
    }

    var accordions = { ship: null };
    root.querySelectorAll(".product__accordion").forEach(function (box) {
      var title = ((box.querySelector(".accordion__title") || {}).textContent || "").trim();
      if (/verzenden|bezorg|shipping|retour/i.test(title)) accordions.ship = box;
      else if (/cadeau|gift|maat|size\s*chart|ruler|klanten\s*service|customer\s*service|klantenservice/i.test(title))
        box.remove();
    });

    function accordionPanel(box) {
      if (!box) return null;
      var body = document.createElement("div");
      body.className = "rte";
      var copy = box.querySelector(".accordion__content");
      if (copy) {
        while (copy.firstChild) body.appendChild(copy.firstChild);
      }
      box.remove();
      if (!body.textContent.trim() && !body.querySelector("a,img,table,ul,ol,p")) return null;
      return body;
    }

    var product = root.querySelector(".product") || root.closest(".product") || root;
    var tabs = document.createElement("div");
    tabs.className = "ktm-tabs";
    var list = document.createElement("div");
    list.className = "ktm-tabs__list";
    list.setAttribute("role", "tablist");
    tabs.appendChild(list);
    var tabsHost = document.querySelector("[data-ktm-tabs-host]");
    if (tabsHost) {
      tabsHost.appendChild(tabs);
    } else {
      product.insertAdjacentElement("afterend", tabs);
    }

    // Geen bijbehorende producten (vaak fietsen): tabs volle breedte.
    var meta = (tabsHost && tabsHost.closest(".ktm-product-meta")) || document.querySelector(".ktm-product-meta");
    if (meta && !meta.querySelector(".ktm-side-relations--mixed")) {
      meta.classList.add("ktm-product-meta--full");
    }

    var selectedId = null;
    function selectTab(id) {
      var buttons = list.querySelectorAll(".ktm-tabs__tab");
      var panels = tabs.querySelectorAll(".ktm-tabs__panel");
      if (!buttons.length) return;
      if (!id || !document.getElementById("ktm-tab-" + id)) {
        id = buttons[0].id.replace("ktm-tab-", "");
      }
      selectedId = id;
      buttons.forEach(function (tab, i) {
        var on = tab.id === "ktm-tab-" + id;
        tab.setAttribute("aria-selected", on ? "true" : "false");
        tab.tabIndex = on ? 0 : -1;
        if (panels[i]) panels[i].hidden = !on;
      });
    }

    var shortLabels = {
      info: "Info",
      fits: "Past op",
      frame: "Frame",
      geometry: "Geometrie",
      ship: "Verzending",
    };

    function bikeEnrichment() {
      var el = document.getElementById("ktm-bike-enrichment");
      if (!el) return {};
      try {
        return JSON.parse(el.textContent || "{}") || {};
      } catch (err) {
        return {};
      }
    }

    function buildBikeHighlights() {
      var wh = root.getAttribute("data-bike-battery-wh");
      var motor = root.getAttribute("data-bike-motor");
      var weight = root.getAttribute("data-bike-weight-kg");
      if (!wh && !motor && !weight) return null;
      var box = document.createElement("div");
      box.className = "ktm-bike-highlights";
      var html = "";
      if (wh) {
        html +=
          '<span class="ktm-bike-badge" title="Accucapaciteit">' +
          '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M7 0L3 12H10.5L2 24L22 10H12L19 0H7Z"/></svg>' +
          esc(wh) +
          "Wh</span>";
      }
      if (motor) {
        html += '<span class="ktm-bike-spec">' + esc(motor) + "</span>";
      }
      if (weight) {
        html +=
          '<span class="ktm-bike-spec">' +
          esc(String(weight).replace(".", ",")) +
          " kg</span>";
      }
      box.innerHTML = html;
      return box;
    }

    function buildFrameSizesPanel() {
      var src = root.getAttribute("data-frame-sizes-src");
      if (!src) return null;
      var box = document.createElement("div");
      box.className = "ktm-frame-sizes";
      box.innerHTML =
        '<p class="ktm-frame-sizes__note">Let op: deze tabel geeft alleen een globale indicatie.</p>' +
        '<img class="ktm-frame-sizes__img" src="' +
        esc(src) +
        '" alt="KTM frame maten naar lichaamslengte" width="1110" height="625" loading="lazy" decoding="async">';
      return box;
    }

    // Same A–L legend as ktm-bikes.at (geo-bullet next to each row).
    var GEO_LETTER_BY_LABEL = {
      size: "A",
      "seattube length": "B",
      "toptube length": "C",
      "seat angle": "D",
      "head angle": "E",
      "headtube length": "F",
      "chainstay length": "G",
      wheelbase: "H",
      stack: "I",
      reach: "J",
      "standover height": "K",
      "bb drop": "L",
    };

    function geometryLabelHtml(label) {
      var letter = GEO_LETTER_BY_LABEL[String(label || "").trim().toLowerCase()];
      if (!letter) return esc(label);
      return (
        '<span class="ktm-geo-letter" aria-hidden="true">' +
        letter +
        '</span><span class="ktm-geo-label-text">' +
        esc(label) +
        "</span>"
      );
    }

    function buildGeometryPanel() {
      var data = bikeEnrichment().geometry;
      var imgSrc = (root.getAttribute("data-geometry-img") || "").trim();
      var sizes = data && typeof data === "object" ? Object.keys(data) : [];
      if (!sizes.length && !imgSrc) return null;
      var box = document.createElement("div");
      box.className = "ktm-bike-geometry";
      var html = "";
      if (imgSrc) {
        html +=
          '<div class="ktm-bike-geometry__visual">' +
          '<img class="ktm-bike-geometry__img" src="' +
          esc(imgSrc) +
          '" alt="Geometrie" width="555" height="378" loading="lazy" decoding="async">' +
          "</div>";
      }
      if (sizes.length) {
        html += '<div class="ktm-bike-geometry__data">';
        html += '<div class="ktm-bike-geometry__tabs" role="tablist">';
        var panelsHtml = "";
        sizes.forEach(function (size, index) {
          var selected = index === 0;
          html +=
            '<button type="button" class="ktm-bike-geometry__tab" role="tab" data-size="' +
            esc(size) +
            '" aria-selected="' +
            (selected ? "true" : "false") +
            '">' +
            esc(size) +
            "</button>";
          panelsHtml +=
            '<div class="ktm-bike-geometry__panel" data-size="' +
            esc(size) +
            '"' +
            (selected ? "" : " hidden") +
            "><table><tbody>";
          var rows = data[size] || {};
          Object.keys(rows).forEach(function (label) {
            panelsHtml +=
              "<tr><th>" +
              geometryLabelHtml(label) +
              "</th><td>" +
              esc(rows[label]) +
              "</td></tr>";
          });
          panelsHtml += "</tbody></table></div>";
        });
        html += "</div>" + panelsHtml + "</div>";
      }
      box.innerHTML = html;
      box.querySelectorAll(".ktm-bike-geometry__tab").forEach(function (tab) {
        tab.addEventListener("click", function () {
          var size = tab.getAttribute("data-size");
          box.querySelectorAll(".ktm-bike-geometry__tab").forEach(function (t) {
            t.setAttribute("aria-selected", t === tab ? "true" : "false");
          });
          box.querySelectorAll(".ktm-bike-geometry__panel").forEach(function (panel) {
            panel.hidden = panel.getAttribute("data-size") !== size;
          });
        });
      });
      return box;
    }

    function stripLegacySpecLinks(node) {
      if (!node || !node.querySelectorAll) return node;
      node.querySelectorAll("tr").forEach(function (row) {
        var label = ((row.querySelector("th,td") || {}).textContent || "")
          .replace(/\s+/g, " ")
          .trim()
          .toLowerCase();
        var text = (row.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
        if (
          label === "geometrie" ||
          label === "frame size" ||
          text.indexOf("klik hier voor de geometrie") !== -1 ||
          text.indexOf("klik hier voor de juiste maatvoering") !== -1
        ) {
          row.remove();
        }
      });
      return node;
    }

    function enrichDescription(node) {
      if (!node) return node;
      node = stripLegacySpecLinks(node);
      var marketing = bikeEnrichment().marketing;
      var highlights = buildBikeHighlights();
      if (!marketing && !highlights) return node;

      var wrap = document.createElement("div");
      wrap.className = "ktm-bike-info";

      var headerLeft = highlights || document.createElement("div");
      if (!highlights) {
        headerLeft.className = "ktm-bike-highlights ktm-bike-highlights--spacer";
      }
      wrap.appendChild(headerLeft);

      var specsTitle = document.createElement("div");
      specsTitle.className = "ktm-bike-info__specs-title";
      specsTitle.innerHTML = '<span class="ktm-bike-spec">Specificaties</span>';
      wrap.appendChild(specsTitle);

      var copy = document.createElement("div");
      copy.className = "ktm-bike-marketing";
      if (marketing) {
        String(marketing)
          .split(/\n\s*\n/)
          .map(function (part) {
            return part.replace(/\s+/g, " ").trim();
          })
          .filter(Boolean)
          .forEach(function (part) {
            var p = document.createElement("p");
            p.textContent = part;
            copy.appendChild(p);
          });
      }
      wrap.appendChild(copy);

      var specs = document.createElement("div");
      specs.className = "ktm-bike-info__specs";
      specs.appendChild(node);
      wrap.appendChild(specs);

      return wrap;
    }

    function applyTabLabels() {
      var short = window.matchMedia("(max-width: 749px)").matches;
      list.querySelectorAll(".ktm-tabs__tab").forEach(function (tab) {
        var full = tab.dataset.labelFull || tab.textContent;
        var tiny = tab.dataset.labelShort;
        tab.textContent = short && tiny ? tiny : full;
      });
    }

    function addTab(id, labelText, node, position) {
      if (!node) return;
      if (document.getElementById("ktm-tab-" + id)) return;
      var tab = document.createElement("button");
      tab.type = "button";
      tab.className = "ktm-tabs__tab";
      tab.id = "ktm-tab-" + id;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-controls", "ktm-panel-" + id);
      tab.dataset.labelFull = labelText;
      if (shortLabels[id]) tab.dataset.labelShort = shortLabels[id];
      tab.addEventListener("click", function () {
        selectTab(id);
      });
      var panel = document.createElement("div");
      panel.className = "ktm-tabs__panel";
      panel.id = "ktm-panel-" + id;
      panel.setAttribute("role", "tabpanel");
      panel.setAttribute("aria-labelledby", tab.id);
      panel.appendChild(node);
      if (node.classList) node.classList.remove("hidden");
      var tabAt = position === 1 ? list.children[1] : null;
      var panelAt = position === 1 ? tabs.children[2] : null;
      list.insertBefore(tab, tabAt || null);
      tabs.insertBefore(panel, panelAt || null);
      applyTabLabels();
      selectTab(selectedId || "info");
    }

    addTab("info", "Productinformatie", enrichDescription(description));
    var fits = buildFitsTable();
    if (fits) addTab("fits", "Past op deze modellen", fits, 1);
    var geometryPanel = buildGeometryPanel();
    if (geometryPanel) addTab("geometry", "Geometrie", geometryPanel);
    var framePanel = buildFrameSizesPanel();
    if (framePanel) addTab("frame", "Frame maten", framePanel);
    addTab("ship", "Verzending & retour", accordionPanel(accordions.ship));

    window.addEventListener("resize", applyTabLabels);
  })();
})();
