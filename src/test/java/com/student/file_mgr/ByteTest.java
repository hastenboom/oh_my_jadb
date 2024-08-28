package com.student.file_mgr;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class ByteTest {


    @Test
    void testLongAndInt() {
        Byte byte_ = new Byte(100);

        var long_exp = 200L;
        byte_.putLong(2, long_exp);
        long long_act = byte_.getLong(2);

        assertEquals(long_exp, long_act);

        log.info("{}", byte_);

        var int_exp = 300;
        byte_.putInt(15, int_exp);
        int int_act = byte_.getInt(15);
        assertEquals(int_exp, int_act);
        log.info("{}", byte_);
    }

    @Test
    void testString() {
        Byte aByte = new Byte(100);
        val strOffset = 10;
        val str_exp = "Hello World";

        aByte.putString(strOffset, str_exp);
        val str_act = aByte.getString(strOffset);
        assertEquals(str_exp, str_act);
    }

    @Test
    void testByteArr() {
        Byte aByte = new Byte(100);
        val arrOffset = 10;

        val arr_exp = new byte[]{1, 2, 3, 4, 5};
        aByte.putByteArr(arrOffset, arr_exp);
        val arr_act = aByte.getByteArr(arrOffset);

        assertArrayEquals(arr_exp, arr_act);
    }

    @Test
    void testSize(){
        Byte aByte = new Byte(100);
        aByte.putInt(0,100);

        System.out.println(aByte.getByteBuffer().position());


    }
}