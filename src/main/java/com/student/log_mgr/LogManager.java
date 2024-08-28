package com.student.log_mgr;

import com.student.file_mgr.Block;
import com.student.file_mgr.Byte;
import com.student.file_mgr.FileManager;
import lombok.*;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.student.Constants.INT_LEN;

/**
 *
 * logFile layout:
 * |Blk0|Blk1|Blk2|
 * -----------------
 * BlkX layout:
 * |whereToWrite|empty|empty|lr1Len|lr1|lr0Len|lr0|
 * |4B          |     |     |4B    |var|4B    |var|
 *                          ^ whereToWrite
 *  The log file block is written in a reverse order, the initial whereToWrite is at the end of the block. Suppose
 *  the offset of the end of the block is 400, and lr0 is 100B, then the whereToWrite should be updated to 300
 *
 * @author Student
 */
@Getter
@Setter
public class LogManager extends FileManager implements Iterable<byte[]> {

    private final String logFileName;

    private Byte logByte;

    private Block curBlk;

    private int latestLSN;

    private int lastSavedLSN;

    private final Lock logLock;


    public LogManager(String dbDir, int blockSize, String logFileName) {
        super(dbDir, blockSize);

        this.logFileName = logFileName;
        this.logByte = new Byte(blockSize);
        this.latestLSN = 0;
        this.lastSavedLSN = 0;
        this.logLock = new ReentrantLock();

        int blockCount = super.getBlockCount(logFileName);

        if (blockCount == 0) {
           /* Block block = super.appendNewBlock(logFileName);
            //set the whereToWrite
            logByte.putInt(0, blockSize);
            super.writeToBlock(block, logByte, true);
            curBlk = block;*/
            curBlk = this.appendNewLogBlock(logFileName);
        }
        else {
            //if not empty, read the last block
            this.curBlk = new Block(logFileName, blockCount - 1);
            this.logByte = super.readFromBlock(curBlk);
        }
    }


    public int writeLogRecord(byte[] logRecord) {
        logLock.lock();
        try {
            /*
             * |whereToWrite|empty|empty|lr1Len|lr1|lr0Len|lr0|
             * |4B          |     |     |4B    |var|4B    |var|
             *                          ^ whereToWrite
             * */
            var whereToWrite = logByte.getInt(0);

            val wholeLrLen = logRecord.length + INT_LEN;

            //insufficient space in the current block
            if ((whereToWrite - wholeLrLen) < INT_LEN) {
                this.flush();//flush the logByte to block
                this.curBlk = this.appendNewLogBlock(logFileName);
                whereToWrite = logByte.getInt(0);
            }

            val lrOffset = whereToWrite - wholeLrLen;

            this.logByte.putByteArr(lrOffset, logRecord);// also add the lrLen
            this.logByte.putInt(0, lrOffset);//update whereToWrite

            this.latestLSN += 1;
            return latestLSN;
        }
        finally {
            logLock.unlock();
        }
    }

    public void flush() {
        super.writeToBlock(curBlk, logByte, true);
        logByte.clear();

    }

    public void flushByLSN(int lsn) {
        if (lsn > this.lastSavedLSN) {
            this.flush();
            this.lastSavedLSN = lsn;
        }
    }


    /**
     * create a new block with whereToWrite info
     * @param logFileName
     * @return
     */
    private Block appendNewLogBlock(String logFileName) {
        Block block = super.appendNewBlock(logFileName);

        //set the whereToWrite
        this.logByte.putInt(0, super.getBlockSize());
        /*我这里实际上只想写入这个int，但是会把整个byte都写进去，所以会出现我的错误*/
        super.writeToBlock(block, logByte, true);
//        curBlk = block;
        return block;
    }


    @NotNull
    @Override
    public Iterator<byte[]> iterator() {
        this.flush();
        return new LogIterator(this, this.curBlk);
    }
}
