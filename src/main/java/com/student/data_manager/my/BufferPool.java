package com.student.data_manager.my;

import org.jetbrains.annotations.Nullable;

/**
 * @author Student
 */
public interface BufferPool<T> {

    T get(long id);

    /**
     * 🤔 should I remove it?
     * @param id
     * @param obj
     * @return
     */
    T put(long id, T obj);

    void free(long id);

    @Nullable
    T getIfCacheMiss(long id);

    void writeBackCache(T obj);

    /**
     * close the buffer pool and write back all dirty pages to the next level disk
     */
    void close();
}
