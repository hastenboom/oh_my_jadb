package com.student.data_manager.my.page;

/**
 * TODO
 * @author Student
 */
public class DefaultCachePage implements CachePage {

    //starting from 0
    private long pageNumber;

    private byte[] data;

    private boolean dirty;

    public DefaultCachePage(long pageNumber, byte[] data) {
        this.pageNumber = pageNumber;
        this.data = data;
//        this.dirty = dirty;
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
