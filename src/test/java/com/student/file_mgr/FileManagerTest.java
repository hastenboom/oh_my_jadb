package com.student.file_mgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;


class FileManagerTest {

    @Test
    public void testReadWrite() throws IOException {
        int blockSize = 20;
        FileManager fm = new FileManager("fileMgrTest", blockSize);
        Block blk = new Block("test.txt", 1);

        Byte aByte = new Byte(blockSize);
        /*
        Byte layout:
        |123|Hello!|
        |4B |6B   |
         */
        aByte.putInt(2, 123);
        aByte.putString(6, "Hello!");

        fm.writeToBlock(blk, aByte, false);
        Byte aByte1 = fm.readFromBlock(blk);

        Assertions.assertArrayEquals(aByte.getContent(), aByte1.getContent());

        fm.appendNewBlock("test.txt");
    }

    @Test
    public void testReadWrite2() throws IOException {
        int blockSize = 20;
        FileManager fm = new FileManager("fileMgrTest", blockSize);
        Block blk = new Block("test2.txt", 0);

        Byte aByte = new Byte(blockSize / 2);
        /*
        Byte layout:
        |123|Hello!|
        |4B |6B   |
         */

//        aByte.putInt(1, 123);
        aByte.putString(0, "abc");
//        aByte.putString(5, "h");
        fm.writeToBlock(blk, aByte, false);

        Byte aByte1 = fm.readFromBlock(blk);
        System.out.println(Arrays.toString(aByte1.getContent()));
      /*


        Assertions.assertArrayEquals(aByte.getContent(), aByte1.getContent());

        fm.appendNewBlock("test.txt");*/
    }

}