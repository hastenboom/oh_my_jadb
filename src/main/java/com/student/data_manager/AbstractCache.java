package com.student.data_manager;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Student
 */
public abstract class AbstractCache<T> {

    private Map<Long, T> cache;
    private Map<Long, Integer> refCount;
    private Integer capacity;
    private Integer count;

    public AbstractCache(Integer capacity) {
        this.capacity = capacity;
        cache = new ConcurrentHashMap<>();
        refCount = new ConcurrentHashMap<>();
        count = 0;//TODO: should be removed?
    }


    //get and malloc
    public T get(Long key) {

        T obj = cache.get(key);

        //DCL
        if (obj == null) {
            if (this.capacity > 0 && count >= this.capacity) {
                throw new RuntimeException("Cache is full");
            }

            T lockObj = cache.computeIfAbsent(key, k -> (T) new Object());

            synchronized (lockObj) {
                obj = cache.get(key);
                if (obj == null) {
                    obj = getIfCacheMiss(key);
                    if (obj == null) {
                        throw new RuntimeException("The next level storage devices doesn't have data for key:  " + key);
                    }
                    cache.put(key, obj);
                    this.count++;
                }
            }
        }

        refCount.put(key, refCount.getOrDefault(key, 0) + 1);

        return obj;
    }

    //dealloc
    public void release(Long key) {

        if (cache.get(key) == null) {
            throw new RuntimeException("The key: " + key + " is not in the cache");
        }

        synchronized (cache.get(key)) {
            refCount.put(key, refCount.getOrDefault(key, 0) - 1);
            if (refCount.get(key) == 0) {
                Object obj = cache.get(key);
                releaseAndWriteBack( obj);
                refCount.remove(key);
                cache.remove(key);
            }

        }
    }

    /**
     * 1. write back all the data in the cache to next level storage devices
     * <p>
     * 2. clear the cache
     */
    public void closeCache() {
        //lock the whole Cache instance, not the cache map
        synchronized (this) {
            for (Map.Entry<Long, T> entry : cache.entrySet()) {
                releaseAndWriteBack(entry.getValue());
                refCount.remove(entry.getKey());
                cache.remove(entry.getKey());
            }
        }
    }


    @Nullable
    abstract public T getIfCacheMiss(Long key);

    abstract public void releaseAndWriteBack(Object value);
}
