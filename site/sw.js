const CACHE_NAME = 'vasanerd-v3';
const DATA_CACHE = 'vasanerd-data-v10';

// Cache data files (JSON) with cache-first strategy
// Cache index.html with network-first strategy

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME && k !== DATA_CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Only handle GET requests
  if (e.request.method !== 'GET') return;

  // Data files: stale-while-revalidate (serve cached, update in background)
  if (url.pathname.match(/\/data\/.*\.json$/)) {
    e.respondWith(
      caches.open(DATA_CACHE).then(cache =>
        cache.match(e.request).then(cached => {
          const networkFetch = fetch(e.request).then(resp => {
            if (resp.ok) cache.put(e.request, resp.clone());
            return resp;
          });
          return cached || networkFetch;
        })
      )
    );
    return;
  }

  // CDN scripts (Chart.js): cache-first
  if (url.hostname === 'cdn.jsdelivr.net') {
    e.respondWith(
      caches.open(CACHE_NAME).then(cache =>
        cache.match(e.request).then(cached => {
          if (cached) return cached;
          return fetch(e.request).then(resp => {
            if (resp.ok) cache.put(e.request, resp.clone());
            return resp;
          });
        })
      )
    );
    return;
  }

  // Manifest & icons: cache-first
  if (url.pathname === '/manifest.json' || url.pathname.startsWith('/icons/') || url.pathname === '/favicon.ico') {
    e.respondWith(
      caches.open(CACHE_NAME).then(cache =>
        cache.match(e.request).then(cached => {
          if (cached) return cached;
          return fetch(e.request).then(resp => {
            if (resp.ok) cache.put(e.request, resp.clone());
            return resp;
          });
        })
      )
    );
    return;
  }

  // HTML: network-first (always get latest)
  if (url.pathname === '/' || url.pathname.endsWith('.html') || url.pathname.startsWith('/p/')) {
    e.respondWith(
      fetch(e.request).then(resp => {
        if (resp.ok) {
          const clone = resp.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
        }
        return resp;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
});
