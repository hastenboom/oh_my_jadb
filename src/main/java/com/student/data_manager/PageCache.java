package com.student.data_manager;

/**
 * @author Student
 */
public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13;// 8KB

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);
}
