package com.stu.dm.logger;

import com.stu.common.FileManager;
import com.stu.common.util.Parser;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class LoggerImplTest {
    public static final String DIR = "log_test";
    public static final int lrNum = 3;

    /*TODO: change the logback.xml into info if you want to check the info file*/
    @Test
    public void testLog() throws FileNotFoundException {

        //change the isNewFile when you test
        LoggerImpl logger = new LoggerImpl(DIR, "log3.log", true);

        ArrayList<byte[]> logRecord_exp = new ArrayList<>();

        for (int i = 0; i < lrNum; i++) {
            val lr = ("lr" + i).getBytes();
            logRecord_exp.add(lr);
            logger.log(lr);
        }


        ArrayList<byte[]> logRecord_act = new ArrayList<>();

        for (var it : logger) {
            logRecord_act.add(it);
        }

        for (int i = 0; i < lrNum; i++) {
            assertEquals(new String(logRecord_exp.get(i)), new String(logRecord_act.get(i)));
        }

        logger.close();
    }

}