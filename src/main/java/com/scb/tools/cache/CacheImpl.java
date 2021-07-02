package com.scb.tools.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class CacheImpl<K, V> implements Cache<K, V> {

    private final Map<Optional<K>, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final Function<K, V> valueLoader;
    // mutex used to create new CacheEntry objects
    private final ReentrantLock lock = new ReentrantLock();

    public CacheImpl(Function<K, V> valueLoader) {
        if (valueLoader == null)
            throw new IllegalArgumentException("Value loader cannot be null!");

        this.valueLoader = valueLoader;
    }

    @Override
    public V get(K key) {
        Optional<K> optionalKey = Optional.ofNullable(key);

        // First we need to create a CacheEntry object for this key, if it does not exist.
        // Because we want to avoid using putIfAbsent/computeIfAbsent, we have to use a common lock object here
        lock.lock();
        try {
            CacheEntry<V> current = cache.get(optionalKey);
            if (current == null)
                cache.put(optionalKey, new CacheEntry<>());
        } finally {
            lock.unlock();
        }

        // Now we can lock on a specific key
        synchronized (cache.get(optionalKey)) {
            CacheEntry<V> current = cache.get(optionalKey);
            if (!current.isComputed()) {
                try {
                    current.setValue(valueLoader.apply(key));
                    current.setComputed(true);
                } catch (Exception e) {
                    // It can be useful here to have this line of log, to make it more obvious for the user
                    // that the error comes from within the value loader
                    System.err.printf("Value loader threw an exception! Input was: %s%n", key);
                    // but after logging, we rethrow to let the user see it and fix it
                    throw e;
                }
            }
        }

        return cache.get(optionalKey).getValue();
    }

    private static class CacheEntry<V> {

        private V value;
        private boolean computed;

        public CacheEntry() {
            this.value = null;
            this.computed = false;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public boolean isComputed() {
            return computed;
        }

        public void setComputed(boolean computed) {
            this.computed = computed;
        }

    }

}
