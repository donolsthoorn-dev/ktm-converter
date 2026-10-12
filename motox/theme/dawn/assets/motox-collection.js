/* Motox collectiepagina */
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

  function placeWissen() {
    var form = document.querySelector(
      '.facets-vertical .facets__form-vertical, .facets-vertical form, #FacetFiltersForm'
    );
    if (!form) return;
    var heading = form.querySelector('.facets__heading--vertical, .facets__heading');
    if (!heading || heading.querySelector('.motox-facets-wissen')) return;
    var clearLink =
      form.querySelector('.active-facets__button-wrapper a') ||
      form.querySelector('a.facets__reset');
    var a = document.createElement('a');
    a.className = 'motox-facets-wissen';
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

  /* Fallback als garage-app het vinkje niet toont (proxy down). */
  function ensureMotorToggle() {
    if (!document.querySelector('.motox-collection-section, .motox-facets-layout, .template-collection')) return;
    if (document.getElementById('mxg-filter-toggle')) return;
    if (/^\/collections\/atv/i.test(location.pathname || '')) return;
    var vehicle = readGarage();
    if (!vehicle) return;
    var mobile = window.matchMedia('(max-width: 749px)').matches;
    var host = document.getElementById('ProductGridContainer');
    var form = document.querySelector(
      '.motox-collection-toolbar .facets-vertical-form, #FacetSortForm, .facets-container'
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

  function run() {
    var onCollection =
      document.body.classList.contains('template-collection') ||
      document.body.classList.contains('template-search') ||
      !!document.querySelector('.motox-collection-section');
    if (!onCollection) return;
    syncAtvFilterPolicy();
    placeWissen();
    watchGarageEmpty();
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
