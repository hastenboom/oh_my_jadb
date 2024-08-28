
package com.stu.dm.cache;


import com.stu.dm.page.Page;
import com.stu.dm.page.PageImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.stu.dm.PageCacheConfig.PAGE_SIZE;


/**
 * TODO:🤔 a single buffer? No! It extends the AbstractCache, so it's a bufferPool. But why does it link to a file?
 * The file is possibly the DataBase file, not the table file;
 * @author Student
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private AtomicInteger pageNumbers;
    private Lock fileLock;

    public PageCacheImpl(RandomAccessFile file, FileChannel fc) {
        this(file, fc, MEM_MIN_LIM);
    }

    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int capacity) {
        super(capacity);
        if (capacity < MEM_MIN_LIM) {
            throw new IllegalArgumentException("Capacity must be greater than or equal to " + MEM_MIN_LIM);
        }

        long length = 0;
        try {
            length = file.length();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.file = file;
        this.fileChannel = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);

    }

    @Override
    public int newPage(byte[] data) {

        int pageNum = pageNumbers.incrementAndGet();//+=1
        Page page = new PageImpl(pageNum, data, null);

        flush(page);

        return pageNum;
    }

    @Override
    public Page getPage(int pgNum) throws Exception {
        return get((long) pgNum);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            file.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void release(Page page) {
        super.release((long) page.getPageNumber());
    }

    /*
    maxPgNum = 3

    |Page1|Page2|Page3|Page4|
                      ^ offset
    * */
    @Override
    public void truncateByPgNum(int maxPgNum) {
        long offset = (long) maxPgNum * PAGE_SIZE;
        try {
            file.setLength(offset);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        pageNumbers.set(maxPgNum);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    @Override
    protected Page getForCache(long key) throws Exception {

        int pgNum = (int) key;
        long offset = (long) (pgNum - 1) * PAGE_SIZE;//starting from 1


        //disk IO
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        }
        finally {
            fileLock.unlock();
        }

        return new PageImpl(pgNum, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        //if the page is not modified, no need to flush it to disk
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }


    /*IO*/
    private void flush(Page page) {
        int pgNum = page.getPageNumber();
        long offset = (long) (pgNum - 1) * PAGE_SIZE;

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            fileLock.unlock();
        }

    }
}
