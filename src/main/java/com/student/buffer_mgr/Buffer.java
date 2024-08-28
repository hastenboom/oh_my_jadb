package com.student.buffer_mgr;

import com.student.file_mgr.Block;
import com.student.file_mgr.Byte;
import com.student.file_mgr.FileManager;
import com.student.log_mgr.LogManager;
import lombok.Data;

/**
 * @author Student
 */
@Data
public class Buffer {
    private FileManager fm;
    private Byte aByte;
    private Block blk;

    private LogManager lm;
    private int count;
    private int txnNum;
    private int lsn;


    public Buffer(FileManager fm, LogManager lm) {
        this.fm = fm;
        this.lm = lm;

        txnNum = -1;
        lsn = 0;
        count = 0;

        aByte = new Byte(fm.getBlockSize());
    }

    /*TODO:???*/
    public void setModified(int txnNum, int lsn) {
        this.txnNum = txnNum;
        if (lsn > 0) {
            this.lsn = lsn;
        }
    }

    public boolean isPinned() {
        return count > 0;
    }

    public void pin() {
        count++;
    }

    public void unpin() {
        count--;
    }


    /*
     * before assigning to a new block, the buffer should be flushed
     * */
    public void assignToBlock(Block blk) {
        this.flush();

        this.aByte = this.fm.readFromBlock(blk);

        this.blk = blk;
        this.count = 0;
    }

    public void flush() {

        /*TODO:????*/
        if (this.txnNum > 0) {
            this.lm.flushByLSN(this.lsn);
            this.fm.writeToBlock(this.blk, this.aByte, true);
            this.txnNum = -1;
        }

    }


}
