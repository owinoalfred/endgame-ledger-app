const CACHE = 'endgame-ledger-v9';

const ASSETS = [
  './',
  './index.html',
  './manifest.json',
  './icon-192.png',
  './icon-512.png'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE)
      .then(cache => cache.addAll(ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys =>
        Promise.all(
          keys
            .filter(key => key !== CACHE)
            .map(key => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  if (url.origin !== location.origin) return;

  event.respondWith(
    fetch(event.request)
      .then(response => {
        if (
          response.ok &&
          event.request.method === 'GET'
        ) {
          const copy = response.clone();

          caches.open(CACHE)
            .then(cache => cache.put(event.request, copy))
            .catch(() => {});
        }

        return response;
      })
      .catch(() =>
        caches.match(event.request)
          .then(response =>
            response || caches.match('./')
          )
      )
  );
});

self.addEventListener('message', event => {
  if (event.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

self.addEventListener('notificationclick', event => {
  event.notification.close();

  event.waitUntil(
    clients.matchAll({
      type: 'window',
      includeUncontrolled: true
    }).then(list => {
      for (const client of list) {
        if ('focus' in client) {
          return client.focus();
        }
      }

      return clients.openWindow(
        event.notification.data?.url || './'
      );
    })
  );
});

self.addEventListener('push', event => {
  let data = {
    title: 'Endgame Ledger',
    body: 'New trading alert',
    tag: 'endgame-alert',
    url: './'
  };

  try {
    data = {
      ...data,
      ...event.data.json()
    };
  } catch {}

  event.waitUntil(
    self.registration.showNotification(
      data.title,
      {
        body: data.body,
        tag: data.tag || 'endgame-alert',
        renotify: true,
        data: {
          url: data.url || './'
        },
        icon: './icon-192.png',
        badge: './icon-192.png'
      }
    )
  );
});
