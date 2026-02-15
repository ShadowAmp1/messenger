const CACHE_NAME = 'mini-messenger-static-v2';
const APP_SHELL = [
  '/',
  '/app',
  '/frontend/index.html',
  '/frontend/styles.css',
  '/frontend/app.js',
  '/frontend/manifest.webmanifest',
  '/frontend/icons/icon-192.svg',
  '/frontend/icons/icon-512.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  const url = new URL(event.request.url);
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws/')) {
    event.respondWith(fetch(event.request));
    return;
  }

  const isAppShellRequest = APP_SHELL.includes(url.pathname);
  if (isAppShellRequest) {
    event.respondWith(
      caches.match(event.request).then((cached) => {
        if (cached) return cached;
        return fetch(event.request);
      })
    );
    return;
  }

  event.respondWith(fetch(event.request));
});
