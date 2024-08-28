package com.stu.tx;

import com.stu.common.util.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * XID file layout:
 * |Header|xid1Status|xid2Status|xid3Status|...|
 * |8B    |1B        |1B        |1B        |...|
 *
 * - the header indicates how many xid are stored in the file
 * @author Student
 */

public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;// check the xidCounter
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态

    //活动状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    //已提交状态
    private static final byte FIELD_TRAN_COMMITTED = 1;
    //回滚状态
    private static final byte FIELD_TRAN_ABORTED = 2;

    // super Txn，永远为commited状态
    public static final long SUPER_XID = 0;

    // XID file suffix
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fileChannel;

    private long xidCounter;// the header of the XID file

    private Lock lock;

    /*create a XID file and create an empty header*/
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                throw new IOException("createNewFile(): File already exists: " + f.getAbsolutePath());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            throw new RuntimeException("File cannot be read/written: " + f.getAbsolutePath());
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        /*
        buf layout:
        |header(empty)|
        |8B           |
        * */
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);

        try {
            //从零创建 XID 文件时需要写一个空的 XID 文件头，即设置 xidCounter 为 0，
            // 否则后续在校验时会不合法：
            fc.position(0);
            fc.write(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    //从一个已有的 xid 文件来创建 TM
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            throw new RuntimeException("XID file does not exist: " + f.getAbsolutePath());
        }
        if (!f.canRead() || !f.canWrite()) {
            throw new RuntimeException("File cannot be read/written: " + f.getAbsolutePath());
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            //用来访问那些保存数据记录的文件
            raf = new RandomAccessFile(f, "rw");
            //返回与这个文件有关的唯一FileChannel对象
            fc = raf.getChannel();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fileChannel = fc;
        lock = new ReentrantLock();
        checkXIDCounter();
    }


    /*a.k.a, check the header and the file length consistency*/
    private void checkXIDCounter() {

        long fileLen = 0;

        try {
            fileLen = file.length();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (fileLen < LEN_XID_HEADER_LENGTH) {
            throw new RuntimeException("XID file doesn't have header : " + fileLen);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            //the buf now has the header
            fileChannel.read(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

//     TODO:   this.xidCounter = Parser.parseLong(buf.array());
        this.xidCounter = buf.getLong(0);

//        long end = getXidPosition(this.xidCounter + 1);
        long end = LEN_XID_HEADER_LENGTH + this.xidCounter * XID_FIELD_SIZE;

        if (end != fileLen) {
            throw new RuntimeException("XID file is not consistent: " + fileLen + " vs " + end);
        }
    }


    private void increaseXIDCounter() {
        xidCounter += 1;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
//        try {
//            fileChannel.force(false);
//        }
//        catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    private void updateXID(long xid, byte status) {

        long offset = this.getXidPosition(xid);
        /*
        tmp layout:
        |status|
        |1B    |
        * */
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;

        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //the valid xid always starts from 1, thus the xid0 shouldn't be stored;
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    @Override
    public long begin() {
        lock.lock();

        try {
            long xid = this.xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            increaseXIDCounter();
            return xid;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return getXIDStatus(xid) == FIELD_TRAN_ACTIVE;
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return getXIDStatus(xid) == FIELD_TRAN_COMMITTED;
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return getXIDStatus(xid) == FIELD_TRAN_ABORTED;
    }

    /*close the XID file and fileChannel*/
    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private byte getXIDStatus(long xid) {
        long offset = this.getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        try {
            fileChannel.position(offset);
            fileChannel.read(ByteBuffer.wrap(tmp));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tmp[0];
    }
}
