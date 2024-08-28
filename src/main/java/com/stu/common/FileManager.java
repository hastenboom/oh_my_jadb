package com.stu.common;

import com.student.file_mgr.Block;
import com.student.file_mgr.Byte;
import lombok.val;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Student
 */
public class FileManager {

    private static Map<String, File> fileCache = new HashMap<>();
    public static Map<String, Boolean> isNew = new HashMap<>();
    private static Lock createLock = new ReentrantLock();

    public static File getFile(String dirName, String fileName) {

        val filePath = Paths.get(dirName, fileName).toString();

        prepareDir(dirName, filePath);

        return prepareFile(filePath);
    }

    public static void closeFile(String dirName, String fileName) {
        val filePath = Paths.get(dirName, fileName).toString();
        closeFile(filePath);
    }

    public static void closeFile(String filePath) {
        fileCache.remove(filePath);
        isNew.remove(filePath);
    }

    public static void deleteFile(String dirName, String fileName) {
        val filePath = Paths.get(dirName, fileName).toString();
        deleteFile(filePath);
    }

    public static void deleteFile(String filePath) {
        boolean isDeleted = new File(filePath).delete();
        if (isDeleted) {
            fileCache.remove(filePath);
            isNew.remove(filePath);
        }
    }


    private static void prepareDir(String dirName, String filePath) {
        File dir = new File(dirName);
        if (!dir.exists()) {
            createLock.lock();
            try {
                if (!dir.exists()) {
                    boolean ok = dir.mkdirs();
                    if (!ok) throw new RuntimeException("Failed to create directory: " + dirName);
                    isNew.put(filePath, true);
                }
            }
            finally {
                createLock.unlock();
            }
        }
    }

    //filePath should contain dir + fileName
    private static File prepareFile(String filePath) {

        File file = fileCache.get(filePath);

        if (file == null) {

            file = new File(filePath);

            //DCL
            if (!file.exists()) {
                createLock.lock();
                try {
                    if (!file.exists()) {
                        boolean isCreated = file.createNewFile();
                        fileCache.put(filePath, file);
                        isNew.put(filePath, isCreated);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    createLock.unlock();
                }
            }
        }
        return file;
    }


}

