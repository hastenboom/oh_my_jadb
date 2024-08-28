package com.student.data_manager;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Student
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {


    //The min capacity of the cache in memory
    private static final int MEM_MIN_LIM = 10;
    private static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;

    // the fileChannel is used to read and write data from/to the RandomAccessFile file;
    private FileChannel fileChannel;

    private final Lock fileLock;

    //the number of pages the xxx.db file occupied
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, Integer capacity) {
        super(capacity);
        if (capacity < MEM_MIN_LIM) {
            throw new IllegalArgumentException("Capacity should be greater than or equal to " + MEM_MIN_LIM);
        }

        long fileLen = 0;
        try {
            fileLen = file.length();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.file = file;
        this.fileChannel = fileChannel;
        this.fileLock = new ReentrantLock();

        /*this indicates that the xxx.db is a big file*/
        this.pageNumbers = new AtomicInteger((int) fileLen / PAGE_SIZE);
    }


    @Override
    public @Nullable Page getIfCacheMiss(Long key) {
        int pgno = key.intValue();
        long offset = pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);//slice
        fileLock.lock();

        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            fileLock.unlock();
        }

        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    public void releaseAndWriteBack(Object value) {

    }

    /**
     * create a new page and write it to the xxx.db file
     *
     * @param initData
     * @return the page number of this new page
     */
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flushPage(pg);
        return pgno;
    }

    /**
     * retrieve the page cache from cache
     *
     * @param pgno
     * @return
     * @throws Exception
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return 0;
    }


    /**
     * write back the page to the xxx.db file
     *
     * @param pg
     */
    @Override
    public void flushPage(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            /*nio*/
            /*slice*/
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);

            //write back the page to the xxx.db file
            fileChannel.write(buf);
            //let the OS decide when to write the data to the disk
            fileChannel.force(false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            fileLock.unlock();
        }
    }

    private long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }
}
