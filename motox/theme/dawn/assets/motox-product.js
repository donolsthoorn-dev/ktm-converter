/* Motox productpagina */
(function () {
    if (document.documentElement.dataset.motoxProduct === "1") return;
    var root = document.querySelector("product-info");
    if (!root) return;
    document.documentElement.dataset.motoxProduct = "1";

    var check =
      '<svg viewBox="0 0 12 12" aria-hidden="true"><path fill="currentColor" d="M4.6 8.6 1.8 5.8l.9-.9 1.9 1.9 4.7-4.7.9.9z"/></svg>';
    var cart =
      '<svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M6 6V5a4 4 0 1 1 8 0v1h2.2a1 1 0 0 1 1 .9l.8 9A1 1 0 0 1 17 17H3a1 1 0 0 1-1-1.1l.8-9a1 1 0 0 1 1-.9H6zm2 0h4V5a2 2 0 1 0-4 0v1z"/></svg>';
    var copy = window.motoxStrings || {};
    function decode(value) {
      return String(value)
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"')
        .replace(/&#39;|&apos;/g, "'");
    }
    function txt(key, fallback) {
      var value = copy[key];
      return decode(value == null || value === "" ? fallback : value);
    }
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
    root.querySelectorAll(".product__text.subtitle").forEach(function (el) {
      if (!vendorEl && el.textContent.trim()) vendorEl = el;
    });
    var share = root.querySelector("share-button");
    if (vendorEl) {
      vendorName = vendorEl.textContent.trim();
      var brandRow = document.createElement("div");
      brandRow.className = "motox-brand-row";
      vendorEl.parentElement.insertBefore(brandRow, vendorEl);
      brandRow.appendChild(vendorEl);
      if (share) brandRow.appendChild(share);
      var img = new Image();
      img.alt = vendorName;
      img.src = "/cdn/shop/files/merk-" + vendorHandle(vendorName) + ".png";
      img.addEventListener("load", function () {
        if (!img.naturalWidth) return;
        vendorEl.textContent = "";
        vendorEl.classList.add("motox-vendor");
        vendorEl.appendChild(img);
      });
    } else if (share) {
      var info = root.querySelector(".product__info-container") || root;
      var brandRow = document.createElement("div");
      brandRow.className = "motox-brand-row";
      info.insertBefore(brandRow, info.firstChild);
      brandRow.appendChild(share);
    }

    root.querySelectorAll(".product__text.subtitle").forEach(function (el) {
      if (!el.textContent.trim() && !el.querySelector("img")) el.hidden = true;
    });

    // Alleen metafield global.ymm_summary tonen onder de titel (geen rebuild).
    (function placeYmmSummary() {
      var title = root.querySelector(".product__title");
      if (!title) return;
      var ymm = root.querySelector(".product__ymm");
      if (!ymm || !ymm.textContent.trim()) {
        var tpl = document.getElementById("motox-ymm-template");
        if (!tpl) return;
        ymm = tpl.content.firstElementChild.cloneNode(true);
      }
      if (ymm.previousElementSibling === title) return;
      title.insertAdjacentElement("afterend", ymm);
    })();

    var price = root.querySelector(".price");
    var tax = root.querySelector(".product__tax");
    if (price && price.parentElement) {
      var priceRow = document.createElement("div");
      priceRow.className = "motox-price-row";
      price.parentElement.insertBefore(priceRow, price);
      var priceCol = document.createElement("div");
      priceRow.appendChild(priceCol);
      priceCol.appendChild(price);
      if (tax) {
        tax.textContent = txt("taxIncluded", "Inclusief BTW");
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
        stock.className = "motox-stock " + (etaOn ? "motox-stock--eta" : "motox-stock--in");
        stock.innerHTML =
          '<span class="motox-stock__label"><span class="motox-stock__mark">' +
          check +
          "</span>" +
          esc(etaOn ? txt("expected", "Verwacht") : txt("inStock", "Op voorraad")) +
          "</span>" +
          (etaOn
            ? '<span class="motox-stock__detail">' +
              esc(txt("availableFrom", "leverbaar vanaf __DATE__").replace("__DATE__", etaText)) +
              "</span>"
            : '<span class="motox-stock__detail">' + esc(txt("availableSoon", "Snel beschikbaar")) + "</span>");
        priceRow.appendChild(stock);
      }
      if (etaBox) etaBox.hidden = true;
    }

    var buttonsHost = root.querySelector(".product-form__buttons");
    if (buttonsHost && !root.querySelector(".motox-buy-meta")) {
      var meta = document.createElement("div");
      meta.className = "motox-buy-meta";
      var ids = document.createElement("div");
      ids.className = "motox-buy-meta__ids";
      var sku = root.querySelector(".product__sku");
      if (sku) ids.appendChild(sku);
      var hasArticleTable = sku && sku.querySelector(".motox-article-table");
      if (sku && vendorName && !hasArticleTable) {
        var sep = document.createElement("span");
        sep.className = "motox-buy-meta__sep";
        sep.setAttribute("aria-hidden", "true");
        sep.textContent = "·";
        ids.appendChild(sep);
      }
      if (vendorName && !hasArticleTable) {
        var brand = document.createElement("span");
        brand.className = "motox-meta__brand";
        var brandLabel = document.createElement("span");
        brandLabel.className = "product__label";
        brandLabel.textContent = txt("brand", "Merk:");
        brand.appendChild(brandLabel);
        brand.appendChild(document.createTextNode(" " + vendorName));
        ids.appendChild(brand);
      }
      meta.appendChild(ids);
      buttonsHost.insertAdjacentElement("afterend", meta);
    }

    function setupSideGallery() {
      var gallery = document.querySelector("media-gallery");
      if (!gallery || gallery.dataset.motoxGallery === "1") return;
      gallery.dataset.motoxGallery = "1";

      var viewer =
        gallery.querySelector("[id^='GalleryViewer']") ||
        gallery.querySelector(".slider-mobile-gutter:not(.thumbnail-slider)");
      var thumbs = gallery.querySelector(".thumbnail-slider");
      var list = thumbs ? thumbs.querySelector(".thumbnail-list") : null;
      var items = list ? Array.prototype.slice.call(list.querySelectorAll(".thumbnail-list__item")) : [];
      var visible = items.filter(function (item) {
        return getComputedStyle(item).display !== "none";
      });

      if (!thumbs || !list || visible.length <= 1) {
        gallery.classList.add("motox-gallery--solo");
        if (thumbs) thumbs.hidden = true;
        return;
      }

      gallery.classList.add("motox-gallery--side");
      thumbs.querySelectorAll(".slider-button").forEach(function (button) {
        button.hidden = true;
      });

      if (visible.length > 4) {
        thumbs.classList.add("motox-thumbs--scroll");
        var nav = document.createElement("div");
        nav.className = "motox-thumbs-nav";
        nav.innerHTML =
          '<button type="button" class="motox-thumbs-nav__btn" data-dir="-1" aria-label="' +
          esc(txt("photosPrev", "Vorige foto's")) +
          '"><svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M10 7.2 4.6 12.6l1.4 1.4L10 10l3.9 4 1.5-1.4z"/></svg></button>' +
          '<button type="button" class="motox-thumbs-nav__btn" data-dir="1" aria-label="' +
          esc(txt("photosNext", "Volgende foto's")) +
          '"><svg viewBox="0 0 20 20" aria-hidden="true"><path fill="currentColor" d="M10 12.8 15.4 7.4 14 6l-4 4-3.9-4L4.6 7.4z"/></svg></button>';
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
    }
    setupSideGallery();

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
      var atc = txt("addToCart", "In winkelwagen");
      var ready = label.querySelector("svg") && label.textContent.toLowerCase().indexOf(atc.toLowerCase()) !== -1;
      if (disabled || ready) return;
      stylingSubmit = true;
      label.classList.add("motox-atc");
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
    if (typeof subscribe === "function" && typeof PUB_SUB_EVENTS !== "undefined") {
      subscribe(PUB_SUB_EVENTS.variantChange, function () {
        styleSubmit();
        var taxLine = root.querySelector(".product__tax");
        if (taxLine) taxLine.textContent = txt("taxIncluded", "Inclusief BTW");
      });
    }

    var trustHost = root.querySelector(".motox-buy-meta") || root.querySelector(".product-form__buttons");
    if (trustHost && !root.querySelector(".motox-trust")) {
      var trust = document.createElement("div");
      trust.className = "motox-trust";
      trust.innerHTML =
        '<div class="motox-trust__item"><span class="motox-trust__icon" aria-hidden="true"><svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 8h16v12H3z"/><path d="M19 12h6l4 4v4h-10"/><circle cx="9" cy="22" r="2.2"/><circle cx="23" cy="22" r="2.2"/></svg></span><span><span class="motox-trust__title">' +
        esc(txt("trustShippingTitle", "Snelle levering")) +
        '</span><span class="motox-trust__text">' +
        esc(txt("trustShippingText", "Snel beschikbaar")) +
        '</span></span></div>' +
        '<div class="motox-trust__item"><span class="motox-trust__icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg></span><span><span class="motox-trust__title">' +
        esc(txt("trustExpertsTitle", "Motorsport experts")) +
        '</span><span class="motox-trust__text">' +
        esc(txt("trustExpertsText", "30+ jaar ervaring")) +
        '</span></span></div>' +
        '<div class="motox-trust__item"><span class="motox-trust__icon" aria-hidden="true"><svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 12a8 8 0 1 1 2.2 6.5"/><path d="M8 8v4h4"/></svg></span><span><span class="motox-trust__title">' +
        esc(txt("trustReturnsTitle", "Eenvoudig retourneren")) +
        '</span><span class="motox-trust__text">' +
        esc(txt("trustReturnsText", "30 dagen retour")) +
        "</span></span></div>";
      trustHost.insertAdjacentElement("afterend", trust);
    }

    var description =
      root.querySelector(".product__description") ||
      document.querySelector(".product__info-container .product__description") ||
      document.querySelector(".product__description");
    if (!description) {
      description = document.createElement("div");
      description.className = "product__description rte";
    }
    var accordions = { ship: null, gift: null, service: null };
    root.querySelectorAll(".product__accordion").forEach(function (box) {
      var key = box.getAttribute("data-motox-tab");
      if (key === "ship" || key === "gift" || key === "service") {
        accordions[key] = box;
        return;
      }
      var title = (box.querySelector(".accordion__title") || {}).textContent || "";
      if (/verzenden|bezorg|shipping/i.test(title)) accordions.ship = box;
      else if (/cadeau|gift card/i.test(title)) accordions.gift = box;
      else if (/klantenservice|customer service/i.test(title)) accordions.service = box;
    });

    function accordionPanel(box) {
      if (!box) return null;
      var body = document.createElement("div");
      body.className = "rte";
      var copy = box.querySelector(".accordion__content");
      if (copy) body.appendChild(copy);
      box.remove();
      return body;
    }

    var product = root.closest(".product") || root;
    var tabs = document.createElement("div");
    tabs.className = "motox-tabs page-width";
    var list = document.createElement("div");
    list.className = "motox-tabs__list";
    list.setAttribute("role", "tablist");
    tabs.appendChild(list);
    product.insertAdjacentElement("afterend", tabs);

    var selectedId = null;
    function selectTab(id) {
      var buttons = list.querySelectorAll(".motox-tabs__tab");
      var panels = tabs.querySelectorAll(".motox-tabs__panel");
      if (!buttons.length) return;
      if (!id || !document.getElementById("motox-tab-" + id)) {
        id = buttons[0].id.replace("motox-tab-", "");
      }
      selectedId = id;
      buttons.forEach(function (tab, i) {
        var on = tab.id === "motox-tab-" + id;
        tab.setAttribute("aria-selected", on ? "true" : "false");
        tab.tabIndex = on ? 0 : -1;
        if (panels[i]) panels[i].hidden = !on;
      });
    }

    var shortLabels = {
      info: txt("tabInfoShort", "Info"),
      fits: txt("tabFitsShort", "Past op"),
      ship: txt("tabShippingShort", "Verzending"),
      gift: txt("tabGiftShort", "Cadeaubon"),
      service: txt("tabServiceShort", "Service"),
    };

    function applyTabLabels() {
      var short = window.matchMedia("(max-width: 749px)").matches;
      list.querySelectorAll(".motox-tabs__tab").forEach(function (tab) {
        var full = tab.dataset.labelFull || tab.textContent;
        var tiny = tab.dataset.labelShort;
        tab.textContent = short && tiny ? tiny : full;
      });
    }

    function addTab(id, labelText, node, position) {
      if (document.getElementById("motox-tab-" + id)) return;
      if (!node) {
        node = document.createElement("div");
        node.className = "rte";
      }
      var tab = document.createElement("button");
      tab.type = "button";
      tab.className = "motox-tabs__tab";
      tab.id = "motox-tab-" + id;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-controls", "motox-panel-" + id);
      tab.dataset.labelFull = labelText;
      if (shortLabels[id]) tab.dataset.labelShort = shortLabels[id];
      tab.addEventListener("click", function () {
        selectTab(id);
      });
      var panel = document.createElement("div");
      panel.className = "motox-tabs__panel";
      panel.id = "motox-panel-" + id;
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

    addTab("info", txt("tabInfo", "Productinformatie"), description);
    addTab("ship", txt("tabShipping", "Verzending & retour"), accordionPanel(accordions.ship));
    addTab("gift", txt("tabGift", "Cadeaubon"), accordionPanel(accordions.gift));
    addTab("service", txt("tabService", "Klantenservice"), accordionPanel(accordions.service));

    function findFits() {
      var blocks = document.querySelectorAll(".product-fits, .ymm-container");
      for (var i = 0; i < blocks.length; i++) {
        if (blocks[i].querySelector("table")) return blocks[i];
      }
      return null;
    }

    function ensureFitsTab() {
      if (document.getElementById("motox-tab-fits")) return true;
      var fits = findFits();
      if (!fits) return false;
      var section = fits.closest(".shopify-section");
      addTab("fits", txt("tabFits", "Past op deze modellen"), fits, 1);
      if (
        section &&
        section !== product.closest(".shopify-section") &&
        !section.querySelector(".product-fits, .ymm-container")
      ) {
        section.hidden = true;
      }
      return true;
    }

    (function tryFitsTab(attempt) {
      if (ensureFitsTab() || attempt >= 6) return;
      window.setTimeout(function () {
        tryFitsTab(attempt + 1);
      }, 400);
    })(0);

    var resizeTimer = null;
    window.addEventListener("resize", function () {
      if (resizeTimer) window.clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(applyTabLabels, 150);
    });
  })();
