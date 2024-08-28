package com.stu.dm.page;

import com.stu.common.util.Parser;
import com.student.data_manager.PageCache;

import java.util.Arrays;

import static com.stu.dm.PageCacheConfig.PAGE_SIZE;

/**
 * PageX layout:
 * |free space|empty      |
 * |2B        |PageSize-2B|
 *            ^ init, free space offset
 *<p>
 *  Suppose append data with len of 10B
 *  - then the free space should be updated to 2 + 10 = 12;
 * <p>
 * PageX layout:
 * |free space|data1|empty      |
 * |2B        |10B  |PageSize-2B|
 *                  ^free space offset
 *
 * @author Student
 */
public class PageX {

    private static final short OFFSET_FREE = 0;//2B
    private static final short OFFSET_FREE_LEN = 2;//2B, TODO: offset_free len?
    public static final int MAX_FREE_SPACE = PAGE_SIZE - OFFSET_FREE_LEN;


    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OFFSET_FREE_LEN);
        return raw;
    }

    // used for update the free space value
    private static void setFSO(byte[] raw, short offset) {
        System.arraycopy(Parser.short2Byte(offset), 0, raw, OFFSET_FREE, OFFSET_FREE_LEN);
    }

    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    public static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OFFSET_FREE, OFFSET_FREE_LEN));
    }

    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());

        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }


    public static int getFreeSpaceSize(Page page) {
        return PAGE_SIZE - getFSO(page.getData());
    }

    /*
     * 注意，这个是危险的操作，它允许对数据进行覆盖写
     * |free space|data1|empty      |
     * |2B        |10B  |PageSize-2B|
     *                  ^free space offset
     * offset = 5, rawLen = 10
     * |freeOffset|data1|data2|empty      |
     * |2B        |7B   |10B  |...        |
     * */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short oldFSO = getFSO(page.getData());

        if (oldFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /*
    * 这个操作的行为更是危险，它允许覆盖源数据的同时，不会更新freeOffset;
    * */
    public static void recoverUpdate(Page page, byte[]raw, short offset){
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }


}
