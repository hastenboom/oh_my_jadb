package com.student.data_manager.my;

import com.student.data_manager.my.page.CachePage;
import com.student.data_manager.my.page.DefaultCachePage;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicInteger;

import static com.student.data_manager.my.page.PageConfig.PAGE_SIZE;

/**
 * A bufferPool designed for handle one Database file
 * <p>
 * <p>
 * TODO: the position related should be modified once I design the DB file
 * <ul>
 *     <li>pageNumbers in init</li>
 *     <li>{@code getPageOffset()}</li>
 *     <li>{@code truncateByPgno()}</li>
 *     <li>{@code getPageNumber()}</li>
 * </ul>
 *
 * @author Student
 */
public class DefaultPageCounterBufferPool extends AbstractCounterBufferPool<CachePage> implements PageCacheSupport {

    private final RandomAccessFile file;
    private FileChannel fileChannel;
    private AtomicInteger pageNumbers;

    public DefaultPageCounterBufferPool(RandomAccessFile file, Integer capacity) {
        super(capacity);
        this.file = file;
        this.fileChannel = file.getChannel();

        long length = 0;
        try {
            length = file.length();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    @Override
    public @Nullable CachePage getIfCacheMiss(final long id) {

        final long offset = getPageOffset(id);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        FileLock readLock = null;
        try {
            readLock = fileChannel.lock(offset, PAGE_SIZE, true);
            fileChannel.position(offset);
            int read = fileChannel.read(buf);
            //nothing to be read
            if (read < 0) {
                return null;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                if (readLock != null)
                    readLock.release();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new DefaultCachePage(id, buf.array());
    }

    @Override
    public void writeBackCache(CachePage obj) {
        if (obj.isDirty()) {
            flushPage(obj);
            obj.setDirty(false);
        }
    }

    /*create a new page and flush it into the disk*/
    @Override
    public int newPage(byte[] initData) {
        int pageNum = pageNumbers.incrementAndGet();
        CachePage cachePage = new DefaultCachePage(pageNum, initData);
        this.flushPage(cachePage);
        return pageNum;
    }

    @Override
    public CachePage getPage(int pgno) {
        return super.get(pgno);
    }

    /*TODO: the api should be redesigned*/
    @Override
    public void release(CachePage cachePage) {
        super.free(cachePage.getPageNumber());
    }

    /**
     * db file: |headers|page0|page1|page2|
     * <p>
     * Suppose the maxPgno is 0, then the size should reach to the offset of page1
     *
     * @param maxPgno
     */
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = getPageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(CachePage pg) {
        int pageNum = pg.getPageNumber();
        long offset = getPageOffset(pageNum);

        FileLock writeLock = null;
        try {
            //
            writeLock = fileChannel.lock(offset, pg.getData().length, false);
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(buf);
            fileChannel.force(false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        /*release writeLock*/
        finally {
            if (writeLock != null) {
                try {
                    writeLock.release();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private long getPageOffset(long pageNumber) {
        return pageNumber * PAGE_SIZE;
    }
}