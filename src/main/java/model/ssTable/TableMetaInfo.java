package model.ssTable;

import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * SsTable索引信息
 */
@Data
public class TableMetaInfo {
    /**
     * 版本号
     */
    private long version;

    /**
     * 数据起始位置
     */
    private long dataStart;

    /**
     * 数据长度
     */
    private long dataLen;

    /**
     * 索引起始位置
     */
    private long indexStart;

    /**
     * 索引长度
     */
    private long indexLen;

    /**
     * SsTable中每个data block的大小
     */
    private long partSize;

    /**
     * 将索引信息写入文件
     */
    public void writeToFile(RandomAccessFile file){
        try {
            file.writeLong(partSize);
            file.writeLong(indexLen);
            file.writeLong(indexStart);
            file.writeLong(dataLen);
            file.writeLong(dataStart);
            file.writeLong(version);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从文件中读取索引信息
     */
    public static TableMetaInfo readFromFile(RandomAccessFile file){
        try {
            TableMetaInfo tableMetaInfo = new TableMetaInfo();
            long len = file.length();

            file.seek(len-8);
            tableMetaInfo.setVersion(file.readLong());

            file.seek(len-8*2);
            tableMetaInfo.setDataStart(file.readLong());

            file.seek(len-8*3);
            tableMetaInfo.setDataLen(file.readLong());

            file.seek(len-8*4);
            tableMetaInfo.setIndexStart(file.readLong());

            file.seek(len-8*5);
            tableMetaInfo.setIndexLen(file.readLong());

            file.seek(len-8*6);
            tableMetaInfo.setPartSize(file.readLong());

            return tableMetaInfo;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
