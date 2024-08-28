package com.stu.dm;

import com.google.common.primitives.Bytes;
import com.stu.common.SubArray;
import com.stu.common.util.Parser;
import com.stu.dm.cache.PageCache;
import com.stu.dm.dataitem.DataItem;
import com.stu.dm.logger.Logger;
import com.stu.dm.page.Page;
import com.stu.dm.page.PageX;
import com.stu.tx.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * insert log:
 * |Xid|Insert|Pos|data|
 *<p>
 * update log:
 * |Xid|Update|Pos|oldData|newData|
 *
 * @author Student
 */
@Slf4j
public class Recovery {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    /*
     * |xid||
     * */
    static class InsertLogInfo {
        long xid;
        int pageNum;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNum;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pc) {
        log.info("Recovering ...");

        logger.rewind();

        int maxPageNum = 0;
        for (var logRecord : logger) {
            int pageNum;
            if (isInsertLog(logRecord)) {
                InsertLogInfo insertLogInfo = parseInsertLog(logRecord);
                pageNum = insertLogInfo.pageNum;

            }
            else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(logRecord);
                pageNum = updateLogInfo.pageNum;
            }

            if (pageNum > maxPageNum) {
                maxPageNum = pageNum;
            }
        }
        if (maxPageNum == 0) maxPageNum = 1;

        pc.truncateByPgNum(maxPageNum);
        System.out.println("Truncate to " + maxPageNum + " pages.");

        redoTransactions(tm, logger, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, logger, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }


    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
        logger.rewind();
        for (var logRecord : logger) {
            if (isInsertLog(logRecord)) {
                InsertLogInfo insertLogInfo = parseInsertLog(logRecord);
                long xid = insertLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, logRecord, REDO);
                }
            }
            else if (isUpdateLog(logRecord)) {
                UpdateLogInfo updateLogInfo = parseUpdateLog(logRecord);
                long xid = updateLogInfo.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, logRecord, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        for (var logRecord : logger) {
            if (isInsertLog(logRecord)) {
                InsertLogInfo insertLogInfo = parseInsertLog(logRecord);
                long xid = insertLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(logRecord);
                }
            }
            else if (isUpdateLog(logRecord)) {
                UpdateLogInfo updateLogInfo = parseUpdateLog(logRecord);
                long xid = updateLogInfo.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(logRecord);
                }

            }
        }

        //in a reverse order
        for (var entry : logCache.entrySet()) {
            List<byte[]> logRecordList = entry.getValue();

            for (int i = logRecordList.size() - 1; i >= 0; i--) {
                byte[] logRecord = logRecordList.get(i);
                if (isInsertLog(logRecord)) {
                    doInsertLog(pc, logRecord, UNDO);
                }
                else {
                    doUpdateLog(pc, logRecord, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }


    private static boolean isInsertLog(byte[] logRecord) {
        return logRecord[0] == LOG_TYPE_INSERT;
    }

    /*FIXME: use the real data, not the if-else logic*/
    private static boolean isUpdateLog(byte[] logRecord) {
        return !isInsertLog(logRecord);
    }


    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();

        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);

        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }


    private static UpdateLogInfo parseUpdateLog(byte[] logRecord) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(logRecord, OF_XID, OF_UPDATE_UID));

        long uid = Parser.parseLong(Arrays.copyOfRange(logRecord, OF_UPDATE_UID, OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        updateLogInfo.pageNum = (int) (uid & ((1L << 32) - 1));

        int length = (logRecord.length - OF_UPDATE_RAW) / 2;

        updateLogInfo.oldRaw = Arrays.copyOfRange(logRecord, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(logRecord, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return updateLogInfo;
    }

    private static void doUpdateLog(PageCache pc, byte[] logRecord, int flag) {
        int pageNum;
        short offset;
        byte[] raw;

        UpdateLogInfo xi = parseUpdateLog(logRecord);
        pageNum = xi.pageNum;
        offset = xi.offset;

        if (flag == REDO) {
            raw = xi.newRaw;
        }
        else if (flag == UNDO) {
            raw = xi.oldRaw;
        }
        else {
            throw new IllegalArgumentException("Invalid flag: " + flag);
        }

        Page pg = null;
        try {
            pg = pc.getPage(pageNum);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            PageX.recoverUpdate(pg, raw, offset);
        }
        finally {
            pg.release();
        }

    }


    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pageNum = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pageNum);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        }
        finally {
            pg.release();
        }
    }
}
