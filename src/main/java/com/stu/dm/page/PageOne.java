package com.stu.dm.page;

import cn.hutool.core.util.RandomUtil;
import com.student.data_manager.PageCache;

import java.util.Arrays;

import static com.stu.dm.PageCacheConfig.PAGE_SIZE;

/**
 * VC : validation check
 * <p>
 * PageOne layout:
 * <p>
 * |empty|VC_open|VC_close|
 * |100B |8B     |8B      |
 *       ^       ^ offset=108
 *       offset = 100
 * <p>
 * If a database file opens and closes successfully, the first page should have the same VC_open and VC_close.
 * Otherwise, the recovery log should be applied.
 *
 * @author Student
 */
public class PageOne {

    private static final int OFFSET_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    //once changed, set it as dirty
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OFFSET_VC, LEN_VC);
    }

    //once changed, set it as dirty
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OFFSET_VC, raw, OFFSET_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OFFSET_VC, OFFSET_VC + LEN_VC), Arrays.copyOfRange(raw,
                OFFSET_VC + LEN_VC, OFFSET_VC + 2 * LEN_VC));
    }
}