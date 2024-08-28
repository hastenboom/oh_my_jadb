package com.student.data_manager;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Student
 */
public class PageImpl implements Page {
    // starting from 1
    private int pageNumber;

    //the real byte data of this page
    private byte[] data;

    // dirty page will be written to disk
    private boolean dirty;

    private Lock lock;

    private PageCache pc;

    public PageImpl(int pageNumber, byte[] initData, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public byte[] getData() {
        return new byte[0];
    }
}
