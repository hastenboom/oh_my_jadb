package com.student.log_mgr;

import org.junit.jupiter.api.Test;

class LogManagerTest {

    public final int BLOCK_SIZE = 20;

    @Test
    public void testLog() {

        LogManager lm = new LogManager("logDirTest", BLOCK_SIZE, "log1.txt");
        byte[] bytes = "lr1".getBytes();
//        System.out.println(bytes.length);
        lm.writeLogRecord(bytes);
        lm.writeLogRecord("lr2".getBytes());

    }

    @Test
    public void testReadAndWriteLog() {
        LogManager lm = new LogManager("logDir", BLOCK_SIZE, "log2.txt");

        /*write*/
        lm.writeLogRecord("ab1".getBytes());
        lm.writeLogRecord("ab2".getBytes());
        lm.writeLogRecord("ab3".getBytes());

        /*read*/
        for (byte[] bytes : lm) {
            System.out.println(new String(bytes));
        }

    }

}