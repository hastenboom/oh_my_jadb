package com.student.buffer_mgr;

import org.jetbrains.annotations.Nullable;

/**
 * A general interface for buffer pool.
 * @author Student
 */
public interface BufferPool<T, ID> {

    T get(ID id);


    void free(ID id);

    @Nullable
    T getIfCacheMiss(ID id);

    void writeBackCache(T obj);

    /**
     * close the buffer pool and write back all dirty pages to the next level disk
     */
    void close();
}