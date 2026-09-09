const CACHE='endgame-ledger-v7';
const ASSETS=['./','./index.html','./manifest.webmanifest','./icons/icon-192.png','./icons/icon-512.png'];
self.addEventListener('install',event=>{event.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)).then(()=>self.skipWaiting()))});
self.addEventListener('activate',event=>{event.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()))});
self.addEventListener('fetch',event=>{const u=new URL(event.request.url);if(u.origin!==location.origin)return;event.respondWith(fetch(event.request).then(r=>{if(r.ok&&event.request.method==='GET'){const copy=r.clone();caches.open(CACHE).then(c=>c.put(event.request,copy)).catch(()=>{})}return r}).catch(()=>caches.match(event.request).then(r=>r||caches.match('./'))))});
self.addEventListener('message',event=>{if(event.data?.type==='SKIP_WAITING')self.skipWaiting()});
self.addEventListener('notificationclick',event=>{event.notification.close();event.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(list=>{for(const c of list){if('focus'in c)return c.focus()}return clients.openWindow(event.notification.data?.url||'./')}))});
self.addEventListener('push',event=>{let data={title:'Endgame Ledger',body:'New trading alert',tag:'endgame-alert',url:'./'};try{data={...data,...event.data.json()}}catch{}event.waitUntil(self.registration.showNotification(data.title,{body:data.body,tag:data.tag||'endgame-alert',renotify:true,data:{url:data.url||'./'},icon:'./icons/icon-192.png',badge:'./icons/icon-192.png'}))});
