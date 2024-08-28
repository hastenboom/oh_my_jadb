package com.student.file_mgr;

import lombok.Data;

/**
 * Block describes a block of a file. It contains the block number and the path of the file.
 * @author Student
 */
//@Data
public record Block(String fileName, int blkNum) {
}
