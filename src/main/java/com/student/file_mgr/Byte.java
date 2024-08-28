package com.student.file_mgr;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * byteBuffer
 * |long|strLen|str|byteArrLen|byteArr|int|
 * |8B  |4B    |var|4B        |var    |4B |
 * @author Student
 */
@Data
public class Byte {

    private ByteBuffer byteBuffer;

    private int byteSize;

    public Byte(byte[] bytes) {
        byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteSize = bytes.length;
    }

    public Byte(int size) {
        byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteSize = size;
    }

    public long getLong(int offset) {
        byteBuffer.position(offset);
        return byteBuffer.getLong();
    }

    public void putLong(int offset, long value) {
        byteBuffer.position(offset);
        byteBuffer.putLong(value);
    }

    public int getInt(int offset) {
        byteBuffer.position(offset);
        return byteBuffer.getInt();
    }

    public void putInt(int offset, int value) {
        byteBuffer.position(offset);
        byteBuffer.putInt(value);
    }

    public String getString(int offset) {
        byteBuffer.position(offset);
        int strLen = byteBuffer.getInt();

        byte[] strBytes = new byte[strLen];
        byteBuffer.get(strBytes);
        return new String(strBytes);
    }

    public void putString(int offset, String value) {
        byte[] bytes = value.getBytes();
        int strLen = bytes.length;
        byteBuffer.position(offset);
        byteBuffer.putInt(strLen);
        byteBuffer.put(bytes);
    }


    public byte[] getByteArr(int offset) {
        byteBuffer.position(offset);
        int byteArrLen = byteBuffer.getInt();

        byte[] byteArr = new byte[byteArrLen];
        byteBuffer.get(byteArr);
        return byteArr;
    }

    public void putByteArr(int offset, byte[] value) {
        int byteArrLen = value.length;
        byteBuffer.position(offset);
        byteBuffer.putInt(byteArrLen);
        byteBuffer.put(value);
    }

    public void clear() {
        byteBuffer.clear();
        byteBuffer.put(new byte[byteBuffer.capacity()]);
        byteBuffer.flip();
    }

    public byte[] getContent() {
        return byteBuffer.array();
    }

    public int getSize() {
        return byteBuffer.array().length;
    }
}
