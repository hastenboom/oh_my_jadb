package com.stu.dm.dataitem;

import com.stu.common.SubArray;
import com.stu.dm.page.Page;

/**
 * @author Student
 */
public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();
}
