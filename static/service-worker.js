const VERSION = 'v1.0.3';
const CACHE = `static-${VERSION}`;
const APP_SHELL = [
  '/',
  '/static/styles.css',
  '/static/app.js',
  '/static/manifest.webmanifest',
  '/static/icons/icon-192.png',
  '/static/icons/icon-512.png',
  '/static/images/login-bg.jpg'  // แคชภาพพื้นหลังเพื่อแสดงออฟไลน์
];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(APP_SHELL)));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.map(k => k !== CACHE ? caches.delete(k) : null)))
  );
  self.clients.claim();
});

self.addEventListener('message', (e) => {
  if (e.data && e.data.type === 'SKIP_WAITING') self.skipWaiting();
});

// navigation: network-first (fallback cache) และไม่ยุ่ง POST
self.addEventListener('fetch', (e) => {
  const req = e.request;

  if (req.method !== 'GET') return;

  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req).catch(async () => {
        const cached = await caches.match('/');
        return cached || new Response('<h1>ออฟไลน์</h1>', { headers: {'Content-Type':'text/html'} });
      })
    );
    return;
  }

  // assets: cache-first
  e.respondWith(
    caches.match(req).then(cached => cached || fetch(req).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(req, copy));
      return res;
    }))
  );
});
