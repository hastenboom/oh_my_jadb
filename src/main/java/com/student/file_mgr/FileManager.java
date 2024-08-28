package com.student.file_mgr;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for managing the file system. It also provides support for Reading file and Storing it into mem; And vice
 * versa;
 *
 * <p>
 * FIXME: lock Table should be provided based on the file and the file block. By doing so, the concurrency efficiency
 * increases. I'll handle it once finishing the concurrency manager;
 * </p>
 */
@Slf4j
@Data
public class FileManager {

    private final String dbDir;
    private final int blockSize;//unit, bytes
    private boolean isNew;
    private Map<String, File> fileCache;//filePath->file
    private final ReentrantReadWriteLock lock;
    private final Lock createLock = new ReentrantLock();


    public FileManager(final String dbDir, final int blockSize)
    {
        this.dbDir = dbDir;
        this.blockSize = blockSize;
        this.isNew = false;
        this.fileCache = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();

        File dir = new File(dbDir);
        if (!dir.exists()) {
            this.isNew = true;
            boolean ok = dir.mkdirs();
            if (!ok) {
                throw new RuntimeException("Failed to create directory: " + dbDir);
            }
        }
        //exists
        else {
            try {
                Files.walk(dir.toPath())
                        .filter(Files::isRegularFile)
                        .map(Path::toFile)
                        .forEach(file -> {
                            String name = file.getName();
                            if (name.startsWith("temp")) {
                                if (!file.delete()) {
                                    throw new RuntimeException("Failed to delete temporary file: " + name);
                                }
                            }
                        });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /*TODO: write lock should be append by using the lock table*/
    public void writeToBlock(Block block, Byte aByte, boolean append) {

//        String filePath = Paths.get(dbDir, block.getFileName()).toString();

        val byteBuffer = aByte.getByteBuffer();
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        File file = getFile(block.fileName());

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            // move to the exact position of the block
            raf.seek((long) block.blkNum() * blockSize);
            if (append) {
                //  if append
                raf.write(byteBuffer.array());
            }
            else {
                // clear and add new data
                raf.setLength((long) block.blkNum() * blockSize);
                raf.write(byteBuffer.array());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // throw Exception if the block is not found here, raf.seek(offset);
    public Byte readFromBlock(Block block) {
        String filePath = Paths.get(dbDir, block.fileName()).toString();
        val offset = (long) block.blkNum() * blockSize;

        File file = new File(filePath);
        if (!file.exists()) {
            try {
                throw new IOException("File does not exist: " + filePath);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // 移动文件指针到指定位置
            raf.seek(offset);
            // 读取指定长度的数据
            byte[] data = new byte[blockSize];
            raf.readFully(data);
            return new Byte(data);
        }
        catch (IOException e) {
            log.error("Failed to read block: {}, EOF? ", block, e);
            throw new RuntimeException(e);
        }
    }


    private File getFile(String fileName) {

        val filePath = Paths.get(dbDir, fileName).toString();

        File file = this.fileCache.get(filePath);

        if (file == null) {

            file = new File(filePath);

            //DCL
            if (!file.exists()) {
                createLock.lock();
                try {
                    if (!file.exists()) {
                        boolean isCreated = file.createNewFile();
                        if (!isCreated) {
                            throw new RuntimeException("Failed to create file: " + filePath);
                        }
                        this.fileCache.put(filePath, file);
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

    // append new block to the end of the file and return the new block
    public Block appendNewBlock(String fileName) {

        File file = getFile(fileName);
        int blockCount = (int) (file.length() / blockSize);
        Block newBlock = new Block(fileName, blockCount);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            // 移动文件指针到文件末尾
            raf.seek((long) blockCount * blockSize);
            // 在文件末尾写入一个新的 block
            byte[] newBlockBytes = new byte[blockSize];
            raf.write(newBlockBytes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return newBlock;
    }


    // return 0 if the file is empty
    public int getBlockCount(String fileName) {
        val file = getFile(fileName);
        return (int) (file.length() / blockSize);
    }

}


    /*    @Deprecated
        public RandomAccessFile getFile(String fileName) {

            String path = Paths.get(dbDir, fileName).toString();

            var randomFile = openFiles.get(path);
            if (randomFile != null) {
                return randomFile;
            }

            // Check if the file exists, if not create it
            File file = new File(path);
            if (!file.exists()) {
                try {
                    boolean isCreated = file.createNewFile();
                }
                catch (IOException e) {
                    log.error("Failed to create file: {}", path, e);
                    throw new RuntimeException(e);
                }
            }

            // Open the file for reading and writing
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(file, "rw");
            }
            catch (FileNotFoundException e) {
                log.error("Failed to open file: {}", path, e);
                throw new RuntimeException(e);
            }

            // Add the file to the openFiles map
            openFiles.put(path, raf);

            return raf;
        }


        }*/




