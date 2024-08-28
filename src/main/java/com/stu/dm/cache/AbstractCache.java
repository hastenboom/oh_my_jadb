package com.stu.dm.cache;

import com.stu.common.error.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Student
 */
public abstract class AbstractCache<T> {
    private Map<Long, T> cache;
    private Map<Long, Integer> refCount;

    private final int capacity;
    private int count;
    private Lock lock;

    public AbstractCache(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<>();
        this.refCount = new HashMap<>();
        this.lock = new ReentrantLock();

        this.count = 0;

    }

    protected T get(long key) throws Exception {

        T obj = cache.get(key);
        if (obj == null) {

            //before rebuild the cache, check if the cache is full
            if (count >= capacity) {
                throw Error.CacheIsFullException;
            }

            lock.lock();
            try {
                obj = cache.get(key);

                if (obj == null) {
                    obj = getForCache(key);
                    if (obj == null) {
                        throw Error.DataNotFoundException;
                    }
                    cache.put(key, obj);
                    refCount.put(key, 1);
                    count++;
                }

            }
            finally {
                lock.unlock();
            }

            /*-----*/

            lock.lock();
            try {
                int ref = refCount.get(key) + 1;
                refCount.put(key, ref);
            }
            finally {
                lock.unlock();
            }
        }

        return obj;
    }

    /**
     * release a resource from cache, if the refCount == 0, remove the resource from cache. Otherwise, just reduce the refCount by 1;
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = refCount.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                refCount.remove(key);
                cache.remove(key);
                count--;
            }
            else {
                refCount.put(key, ref);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * TODO: buggy code, the intend is unclear;
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                refCount.remove(key);
                cache.remove(key);
            }
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * cache miss strategy
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * write back strategy
     */
    protected abstract void releaseForCache(T obj);
}
