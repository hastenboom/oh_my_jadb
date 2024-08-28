package com.student.data_manager;

/**
 * @author Student
 */
public interface Page {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
