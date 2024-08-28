package com.student.buffer_mgr;

import com.student.file_mgr.Block;
import com.student.file_mgr.FileManager;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.student.buffer_mgr.BufferPoolConfig.CAPACITY;

/**
 * @author Student
 */
@Getter
@Setter
public class CounterBufferPool {

}

