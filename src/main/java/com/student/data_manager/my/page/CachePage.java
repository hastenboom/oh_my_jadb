package com.student.data_manager.my.page;

/**
 * @author Student
 */
public interface CachePage {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    /**
     * the page has been modified but not written to disk yet.
     * @return
     */
    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
