package com.student.data_manager.my;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Student
 */
public abstract class AbstractCounterBufferPool<T> implements BufferPool<T> {

    private Map<Long, T> cache;
    private Map<Long, Integer> refCount;
    private Integer capacity;
    private Integer count;

    public AbstractCounterBufferPool(Integer capacity) {
        this.capacity = capacity;
        cache = new ConcurrentHashMap<>();
        refCount = new ConcurrentHashMap<>();
        count = 0;//TODO: should be removed?
    }

    @Override
    public T get(long id) {
        T obj = cache.get(id);

        //DCL
        if (obj == null) {
            if (this.capacity > 0 && count >= this.capacity) {
                throw new RuntimeException("Cache is full");
            }

            /*this is safe*/
            @SuppressWarnings("unchecked")
            T lockObj = cache.computeIfAbsent(id, k -> (T) new Object());

            synchronized (lockObj) {
                obj = cache.get(id);
                if (obj == null) {
                    obj = getIfCacheMiss(id);
                    if (obj == null) {
                        throw new RuntimeException("The next level storage devices doesn't have data for pageId:  " + id);
                    }
                    cache.put(id, obj);
                    this.count++;
                }
            }
        }

        refCount.put(id, refCount.getOrDefault(id, 0) + 1);

        return obj;
    }

    @Override
    public void free(long id) {
        //t1 and t2
        if (cache.get(id) == null) {
//            throw new RuntimeException("The pageId: " + pageId + " is not in the cache");
            System.out.println("Skip: The pageId: " + id + " is not in the cache");
            return;
        }

        synchronized (cache.get(id)) {

            if (cache.get(id) == null) {
                System.out.println("Skip: The cache has been free by other thread, pageId: " + id);
                return;
            }

            refCount.put(id, refCount.getOrDefault(id, 0) - 1);

            if (refCount.get(id) == 0) {
                T obj = cache.get(id);
                writeBackCache(obj);
                refCount.remove(id);
                cache.remove(id);
            }
        }
    }

    @Override
    public synchronized void close() {
        for (var entry : cache.entrySet()) {
            writeBackCache(entry.getValue());
            refCount.remove(entry.getKey());
            cache.remove(entry.getKey());
        }

    }

    @Override
    public T put(long id, T obj) {
        return null;
    }

}
