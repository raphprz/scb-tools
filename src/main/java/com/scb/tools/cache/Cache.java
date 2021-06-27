package com.scb.tools.cache;

public interface Cache<K, V> {
    V get(K key);
}
