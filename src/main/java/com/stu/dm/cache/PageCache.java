package com.stu.dm.cache;


import com.stu.dm.page.Page;

/**
 * @author Student
 */
public interface PageCache {


    int newPage(byte[]initData);

    Page getPage(int pgNum) throws Exception;

    void close();

    void release(Page page);

    void truncateByPgNum(int maxPgNum);

    int getPageNumber();

    void flushPage(Page pg);


}
