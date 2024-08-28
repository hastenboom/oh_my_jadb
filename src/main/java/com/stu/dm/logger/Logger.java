package com.stu.dm.logger;

/**
 * @author Student
 */
public interface Logger extends Iterable<byte[]> {

    void log(byte[] data);

    void truncate(long x) throws Exception;

    void rewind();

    void close();
}
