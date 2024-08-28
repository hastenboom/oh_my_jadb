package com.stu.dm.logger;

import com.google.common.primitives.Bytes;
import com.stu.common.FileManager;
import com.stu.common.util.Parser;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Log File layout:
 * |XChecksum|Log1|Log2|...|LogN|BadTail|
 *<p>
 * LogRecord layout:
 * |Size|Checksum|Data  |
 * |4B  |4B      |Size B|
 * - Size, indicates the size of the data, excluding the checksum;
 * @author Student
 */
@Slf4j
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    //For the lr only, not the offset of the entire log file;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;//4
    private static final int OF_DATA = OF_CHECKSUM + 4;//8

    public static final String LOG_SUFFIX = ".log";

    private final RandomAccessFile file;
    private final FileChannel fc;
    private final String filePath;
    private final Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;


    public LoggerImpl(final String dirName, final String fileName, boolean isNewFile) {
        this(dirName, fileName, 0, isNewFile);
    }

    public LoggerImpl(final String dirName, final String fileName, final int xChecksum, boolean isNewFile) {
        if (!fileName.endsWith(LOG_SUFFIX)) throw new IllegalArgumentException("Invalid log file suffix, use '.log'");


        this.filePath = Paths.get(dirName, fileName).toString();
        File file = FileManager.getFile(dirName, fileName);


        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        this.file = raf;
        this.fc = raf.getChannel();
        this.xChecksum = xChecksum;
        this.lock = new ReentrantLock();

        if (isNewFile) {
            //write the Xchecksum into the file, offset =0, len = 4
            try {
                fc.position(0);
                fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            logFileCheck();
        }

    }

    /*
     * The basic strategy is simple:
     * 1. Read the Xchecksum from the file (stored in field this.xChecksum), offset =0, len = 4
     * 2. Recalculate this value by looping through the entire log file;
     * */
    private void logFileCheck() {
        long size = 0;
        try {
            size = file.length();
            if (size < 4) {//checkSum must exist
                throw new IllegalArgumentException("Invalid log file");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);//read the checkSum;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        //从头遍历所有lr，并计算checksum
        for (var log : this) {
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }

        if (xCheck != xChecksum) {
            throw new IllegalArgumentException("Invalid log file");
        }

        try {
            truncate(position);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            file.seek(position);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        rewind();
    }


    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }


    @Override
    public void log(byte[] data) {
        //the completed log record
        byte[] log = buildLogRecord(data);
        ByteBuffer buf = ByteBuffer.wrap(log);

        lock.lock();
        try {
            //move the the EOF
            fc.position(fc.size());
            fc.write(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            lock.unlock();
        }

        //once insert a lr, update the XChecksum;
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     *build a lr
     *  LogRecord layout:
     * |Size|Checksum|Data  |
     * |4B  |4B      |Size B|
     *
     * */
    private byte[] buildLogRecord(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        }
        finally {
            lock.unlock();
        }
    }


    /*
     *
     *  LogRecord layout:
     * |Size|Checksum|Data  |
     * |4B  |4B      |Size B|
     *
     * */
/*    private byte[] internNext() {

        //TODO: buggy code, when inserting a lr, if the whole lr doesn't fit, it should be inserted;
        if (position + OF_DATA >= fileSize) {
            return null;
        }

        //read size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        int size = Parser.parseInt(tmp.array());

        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        //the whole log record
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] log = buf.array();


        //calculate the checksum via data
        int checksumAct = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length)*//*from, to*//*);
        int checksumExp = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));

        if (checksumAct != checksumExp) {
            return null;
        }

        this.position += log.length;
        return log;
    }*/

/*    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }
        finally {
            lock.unlock();
        }
    }*/

    @Override
    public void rewind() {
        this.position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();

            if (log.isDebugEnabled()) {
                FileManager.deleteFile(filePath);
            }
            else {
                FileManager.closeFile(filePath);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @NotNull
    @Override
    public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {

            public int position = 4;// skip the XChecksum


            @Override
            public boolean hasNext() {
                try {
                    return position < file.length();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            /*
             * Log File layout:
             * |XChecksum|Log1|Log2|...|LogN|BadTail|
             *<p>
             * LogRecord layout:
             * |Size|Checksum|Data  |
             * |4B  |4B      |Size B|
             * - Size, indicates the size of the data, excluding the checksum;
             * */
            @Override
            public byte[] next() {

                //read size
                ByteBuffer lrDataSizeBuf = ByteBuffer.allocate(4);
                try {
                    fc.position(position);
                    fc.read(lrDataSizeBuf);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                int lrDataSize = Parser.parseInt(lrDataSizeBuf.array());

//                if (position + lrDataSize + OF_DATA > fileSize) {
//                    log.warn("Invalid log file, lrSize is too large");
//                    return null;
//                }

                //the whole log record
                ByteBuffer lrBuf = ByteBuffer.allocate(OF_DATA + lrDataSize);
                try {
                    fc.position(position);
                    fc.read(lrBuf);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                byte[] logRecord = lrBuf.array();

                //calculate the checksum via data
                int checksumAct = calChecksum(0, Arrays.copyOfRange(logRecord, OF_DATA, logRecord.length)/*from, to*/);
                int checksumExp = Parser.parseInt(Arrays.copyOfRange(logRecord, OF_CHECKSUM, OF_DATA));

                if (checksumAct != checksumExp) {
                    log.error("invalid checksum for lr :{}", Arrays.toString(logRecord));
                    return null;
                }

                position += logRecord.length;

                if(log.isDebugEnabled()){
                    return Arrays.copyOfRange(logRecord, OF_DATA, logRecord.length);
                }else{
                    return logRecord;
                }

            }
        };
    }
}
