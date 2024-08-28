package com.stu.tx;

/**
 * @author Student
 */
public interface TransactionManager {
    long begin();

    void commit(long xid);

    void abort(long xid);

    boolean isActive(long xid);

    boolean isCommitted(long xid);

    boolean isAborted(long xid);

    void close();


}
