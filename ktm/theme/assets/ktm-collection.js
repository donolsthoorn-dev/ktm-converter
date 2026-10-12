/* KTM collectiepagina */
(function () {
  /* ATV-collecties: motorfilter uit (anders lege ATV-pagina's door motorfiets in garage) */
  function syncAtvFilterPolicy() {
    var onAtv = /^\/collections\/atv/i.test(location.pathname || '');
    try {
      if (onAtv) {
        if (sessionStorage.getItem('mxg-filter-user') === 'on') return;
        if (sessionStorage.getItem('mxg-atv-bypass') === '1') return;
        sessionStorage.setItem('mxg-atv-bypass', '1');
        sessionStorage.setItem('mxg-filter-off', '1');
      } else if (sessionStorage.getItem('mxg-atv-bypass') === '1') {
        sessionStorage.removeItem('mxg-atv-bypass');
        if (sessionStorage.getItem('mxg-filter-user') !== 'off') {
          sessionStorage.removeItem('mxg-filter-off');
        }
      }
    } catch (e) {}
  }
  syncAtvFilterPolicy();

  /* Cadeaubonnen / marketing: niet leegfilteren op motor */
  function skipCollection() {
    var path = location.pathname || '';
    return /\/collections\/(cadeau|gift|sticker|event|marketing)/i.test(path);
  }
  if (skipCollection()) {
    try { sessionStorage.setItem('mxg-filter-off', '1'); } catch (e) {}
  }

  function placeWissen() {
    var form = document.querySelector(
      '.facets-vertical .facets__form-vertical, .facets-vertical form, #FacetFiltersForm'
    );
    if (!form) return;
    var heading = form.querySelector('.facets__heading--vertical, .facets__heading');
    if (!heading || heading.querySelector('.ktm-facets-wissen')) return;
    var clearLink =
      form.querySelector('.active-facets__button-wrapper a') ||
      form.querySelector('a.facets__reset');
    var a = document.createElement('a');
    a.className = 'ktm-facets-wissen';
    a.textContent = 'Wissen';
    if (clearLink && clearLink.href) {
      a.href = clearLink.href;
      a.addEventListener('click', function (e) {
        e.preventDefault();
        clearLink.click();
      });
    } else {
      a.href = window.location.pathname || '#';
      a.style.visibility = 'hidden';
      a.setAttribute('aria-hidden', 'true');
      a.tabIndex = -1;
    }
    heading.appendChild(a);
  }

  function readGarage() {
    try {
      var match = document.cookie.match(/(?:^|;\s*)motox_garage=([^;]*)/);
      if (!match) return null;
      var data = JSON.parse(decodeURIComponent(match[1]));
      if (!data || !data.vehicles || !data.vehicles.length) return null;
      var active = null;
      for (var i = 0; i < data.vehicles.length; i++) {
        if (data.vehicles[i].id === data.activeId) active = data.vehicles[i];
      }
      return active || data.vehicles[data.vehicles.length - 1];
    } catch (e) {
      return null;
    }
  }

  function filterOff() {
    try {
      return sessionStorage.getItem('mxg-filter-off') === '1';
    } catch (e) {
      return false;
    }
  }

  function setFilterOff(off) {
    try {
      if (off) sessionStorage.setItem('mxg-filter-off', '1');
      else sessionStorage.removeItem('mxg-filter-off');
    } catch (e) {}
  }

  /* Fallback als garage-app het vinkje niet toont (proxy down).
     Alleen tonen als er op deze pagina fitment-info staat. */
  function ensureMotorToggle() {
    if (!document.querySelector('.ktm-collection-section, .ktm-facets-layout, .template-collection')) return;
    if (document.getElementById('mxg-filter-toggle')) return;
    if (/^\/collections\/(atv|ktm-bikes|bikes)/i.test(location.pathname || '')) return;
    if (!document.querySelector('.card__ymm, .product__ymm')) return;
    var vehicle = readGarage();
    if (!vehicle) return;
    var mobile = window.matchMedia('(max-width: 749px)').matches;
    var host = document.getElementById('ProductGridContainer');
    var form = document.querySelector(
      '.ktm-collection-toolbar .facets-vertical-form, #FacetSortForm, .facets-container'
    );
    if (mobile ? !host : !form) return;
    var label = document.createElement('label');
    label.className = 'mxg-filter-toggle';
    label.id = 'mxg-filter-toggle';
    var input = document.createElement('input');
    input.type = 'checkbox';
    input.checked = !filterOff();
    input.addEventListener('change', function () {
      setFilterOff(!input.checked);
      var err = document.getElementById('mxg-filter-error');
      if (err) err.remove();
      if (!input.checked) {
        document.documentElement.classList.remove('mxg-hold');
        location.reload();
      } else {
        location.reload();
      }
    });
    var caption = document.createElement('span');
    caption.id = 'mxg-filter-label';
    var name = [vehicle.make, vehicle.model].filter(Boolean).join(' ');
    caption.textContent =
      'Toon alleen onderdelen voor ' + name + (vehicle.year ? ' (' + vehicle.year + ')' : '');
    label.appendChild(input);
    label.appendChild(caption);
    if (mobile && host) host.insertBefore(label, host.firstChild);
    else form.insertBefore(label, form.firstChild);
  }

  /* Lege garage-filterstate: knop i.p.v. alleen “Alles verwijderen” */
  function enhanceGarageEmpty() {
    var empty = document.querySelector('.mxg-filter-empty');
    if (!empty || empty.querySelector('.mxg-show-all')) return;
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'mxg-show-all';
    btn.textContent = 'Toon alle producten in deze collectie';
    btn.addEventListener('click', function () {
      setFilterOff(true);
      var box = document.querySelector('#mxg-filter-toggle input');
      if (box) box.checked = false;
      location.reload();
    });
    empty.appendChild(btn);
  }

  function watchGarageEmpty() {
    var host = document.getElementById('ProductGridContainer') || document.body;
    if (!host || host.__mxgEmptyWatch) return;
    host.__mxgEmptyWatch = true;
    var obs = new MutationObserver(function () {
      enhanceGarageEmpty();
    });
    obs.observe(host, { childList: true, subtree: true });
    enhanceGarageEmpty();
  }

  function vendorHandle(name) {
    return String(name || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '');
  }

  function vendorTone(name) {
    var key = String(name || '')
      .toLowerCase()
      .replace(/[\s\-,]+/g, '');
    if (key.indexOf('gasgas') !== -1) return 'gasgas';
    if (key.indexOf('husqvarna') !== -1) return 'husqvarna';
    if (key === 'wp' || key.indexOf('wp') === 0) return 'wp';
    if (key.indexOf('ktm') !== -1) return 'ktm';
    return 'other';
  }

  /* Vendor → logo op dezelfde plek (ook dynamische garage-kaarten) */
  function enhanceVendorLogos() {
    var nodes = document.querySelectorAll(
      '.product-grid .card-information .caption-with-letter-spacing, .product-grid .card-information__footer .caption-with-letter-spacing'
    );
    Array.prototype.forEach.call(nodes, function (el) {
      if (el.querySelector('.ktm-card-vendor-logo') || el.getAttribute('data-mxg-logo') === '1') {
        if (!el.classList.contains('ktm-card-vendor')) {
          el.classList.add(
            'ktm-card-vendor',
            'ktm-card-vendor--' + vendorTone(el.getAttribute('data-vendor') || el.textContent)
          );
        }
        return;
      }
      var name = (el.getAttribute('data-vendor') || el.textContent || '').trim();
      if (!name) return;
      el.classList.add('ktm-card-vendor', 'ktm-card-vendor--' + vendorTone(name));
      el.setAttribute('data-vendor', name);
      el.setAttribute('data-mxg-logo', '1');
      var handle = vendorHandle(name);
      if (!handle) return;
      var img = document.createElement('img');
      img.className = 'ktm-card-vendor-logo';
      img.alt = name;
      img.loading = 'lazy';
      img.decoding = 'async';
      img.width = 96;
      img.height = 48;
      img.onload = function () {
        if (!img.naturalWidth) return;
        el.classList.add('ktm-card-vendor--has-logo');
      };
      img.onerror = function () {
        if (img.parentNode) img.parentNode.removeChild(img);
        el.removeAttribute('data-mxg-logo');
      };
      var text = el.querySelector('.ktm-card-vendor__text');
      if (!text) {
        text = document.createElement('span');
        text.className = 'ktm-card-vendor__text';
        text.textContent = name;
        el.textContent = '';
        el.appendChild(text);
      }
      el.insertBefore(img, text);
      img.src = '/cdn/shop/files/merk-' + handle + '.png';
    });
  }

  function watchVendorColors() {
    var host = document.getElementById('ProductGridContainer') || document.querySelector('.product-grid');
    if (!host || host.__ktmVendorColorWatch) return;
    host.__ktmVendorColorWatch = true;
    var timer = 0;
    var obs = new MutationObserver(function () {
      window.clearTimeout(timer);
      timer = window.setTimeout(enhanceVendorLogos, 80);
    });
    obs.observe(host, { childList: true, subtree: true });
    enhanceVendorLogos();
  }

  function run() {
    var onCollection =
      document.body.classList.contains('template-collection') ||
      document.body.classList.contains('template-search') ||
      !!document.querySelector('.ktm-collection-section') ||
      !!document.getElementById('ProductGridContainer') ||
      !!document.querySelector('.product-grid');
    if (!onCollection) return;
    syncAtvFilterPolicy();
    placeWissen();
    watchGarageEmpty();
    watchVendorColors();
    // Laat garage.js eerst kans; daarna fallback.
    setTimeout(ensureMotorToggle, 1200);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run);
  else run();
  document.addEventListener('shopify:section:load', run);
  document.addEventListener('click', function (e) {
    if (!e.target.closest('facet-filters-form, .facets-vertical, #FacetFiltersForm')) return;
    setTimeout(run, 400);
  });
})();
