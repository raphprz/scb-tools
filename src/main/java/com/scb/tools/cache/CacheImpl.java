package com.scb.tools.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class CacheImpl<K, V> implements Cache<K, V> {

    private final Map<Optional<K>, Optional<V>> cache = new ConcurrentHashMap<>();
    private final Function<K, V> valueLoader;
    // holds the lock objects per key
    private final Map<Optional<K>, ReentrantLock> locks = new ConcurrentHashMap<>();
    // mutex used to create lock objects per key
    private final ReentrantLock lock = new ReentrantLock();

    public CacheImpl(Function<K, V> valueLoader) {
        if (valueLoader == null)
            throw new IllegalArgumentException("Value loader cannot be null!");

        this.valueLoader = valueLoader;
    }

    @Override
    public V get(K key) {
        Optional<K> optionalKey = Optional.ofNullable(key);

        // First we need to create a lock object for this key, if it does not exist.
        // Because we want to avoid using putIfAbsent/computeIfAbsent, we have to use a common lock object here
        lock.lock();
        try {
            if (!locks.containsKey(optionalKey))
                locks.put(optionalKey, new ReentrantLock());
        } finally {
            lock.unlock();
        }

        // Now we can lock on a specific key
        ReentrantLock lockOnKey = locks.get(optionalKey);
        lockOnKey.lock();
        try {
            if (!cache.containsKey(optionalKey)) {
                Optional<V> value;
                try {
                    value = Optional.ofNullable(valueLoader.apply(key));
                } catch (Exception e) {
                    // It can be useful here to have this line of log, to make it more obvious for the user
                    // that the error comes from within the value loader
                    System.err.printf("Value loader threw an exception! Input was: %s%n", key);
                    // but after logging, we rethrow to let the user see it and fix it
                    throw e;
                }

                cache.put(optionalKey, value);
            }
        } finally {
            lockOnKey.unlock();
        }

        return cache.get(optionalKey).orElse(null);
    }

}
