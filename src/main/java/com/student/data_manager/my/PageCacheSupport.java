package com.student.data_manager.my;

import com.student.data_manager.my.page.CachePage;

/**
 * @author Student
 */
public interface PageCacheSupport {

    /**
     *
     * @param initData
     * @return the newPage number
     */
    int newPage(byte[] initData);

    CachePage getPage(int pgno) throws Exception;

    void close();

    void release(CachePage cachePage);

    void truncateByPgno(int maxPgno);

    int getPageNumber();

    /**
     * write back the page into the disk
     * @param pg
     */
    void flushPage(CachePage pg);

}
