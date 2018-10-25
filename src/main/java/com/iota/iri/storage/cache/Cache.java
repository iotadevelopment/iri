package com.iota.iri.storage.cache;

import com.iota.iri.storage.Indexable;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

public class Cache<T extends Cacheable> {
    protected ConcurrentHashMap<Indexable, CachedWeakReference> cache = new ConcurrentHashMap<>();

    protected final ReferenceQueue<T> releasedCustomObjects = new ReferenceQueue<>();

    public Cache() {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    CachedWeakReference freed;
                    while((freed = (CachedWeakReference) releasedCustomObjects.remove()) != null) {
                        cache.remove(freed.id);
                    }

                    Thread.sleep(10000);
                } catch (InterruptedException e) { /* do nothing */ }
            }
        }, "Cache cleanup thread");
        cleanupThread.start();
    }

    public void add(T o) {
        cache.put(o.getId(), new CachedWeakReference(o));
    }

    public T get(Indexable id) {
        WeakReference<T> weak = cache.get(id);
        if (weak != null) {
            return weak.get();
        }

        return null;
    }

    class CachedWeakReference extends WeakReference<T> {
        private final Indexable id;

        CachedWeakReference(T o) {
            super(o, releasedCustomObjects);

            this.id = o.getId();
        }
    }
}
