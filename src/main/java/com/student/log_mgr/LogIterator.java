package com.student.log_mgr;

import com.student.file_mgr.Block;
import com.student.file_mgr.Byte;
import com.student.file_mgr.FileManager;
import lombok.val;

import java.util.Iterator;

import static com.student.Constants.INT_LEN;

/**
 * @author Student
 */
public class LogIterator implements Iterator<byte[]> {

    public final FileManager fm;
    private Block block;
    private Byte logByte;
    private int curPos;
    private int whereToWrite;

    public LogIterator(FileManager fm, Block block) {
        this.fm = fm;
        this.block = block;

        logByte = new Byte(fm.getBlockSize());
        this.moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        return this.curPos < this.fm.getBlockSize() || block.blkNum() > 0;
    }

    /*
     *  BlkX layout:
     * |whereToWrite|empty|empty|lr1Len|lr1|lr0Len|lr0|
     * |4B          |     |     |4B    |var|4B    |var|
     *                          ^ whereToWrite, curPos
     * */
    @Override
    public byte[] next() {

        //the end of the block, move to the next block
        if (this.curPos >= this.fm.getBlockSize()) {
            /*
             * for shot explanation:
             * |Blk1                    |Blk2                    |
             * |whereToWrite|lr3|lr2|lr1|whereToWrite|lr6|lr5|lr4|
             *             ^where1,pos1              ^where2,po2
             * This shows why block.blkNum() - 1, not + 1
             * */
            moveToBlock(new Block(block.fileName(), block.blkNum() - 1));
        }


        val record = logByte.getByteArr(this.curPos);
        /*
         * |whereToWrite|empty|empty|lr1Len|lr1|lr0Len|lr0|
         * |4B          |     |     |4B    |var|4B    |var|
         *                                     ^ newCurPos
         *                          ^ whereToWrite, oldPos
         * */
        this.curPos += INT_LEN + logByte.getInt(this.curPos);

        return record;
    }


    /*
     *  BlkX layout:
     * |whereToWrite|empty|empty|lr1Len|lr1|lr0Len|lr0|
     * |4B          |     |     |4B    |var|4B    |var|
     *                          ^ whereToWrite
     * */
    private void moveToBlock(Block block) {
        this.logByte = fm.readFromBlock(block);
        this.whereToWrite = this.logByte.getInt(0);
        this.curPos = whereToWrite;// read from the newest to the oldest
        this.block = block;
    }
}
