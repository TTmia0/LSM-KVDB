package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import model.command.Command;
import model.command.RmCommand;
import model.command.SetCommand;
import model.ssTable.SsTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ConvertUtils;
import utils.LoggerUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KV存储实现
 */
public class KvStoreImpl implements KvStore{

    public static final Logger LOGGER = LoggerFactory.getLogger(KvStoreImpl.class);
    public static final String WAL = "wal";
    public static final String WAL_TMP = "wal_tmp";
    public static final String TABLE = ".table";
    public static final String RW = "rw";

    /**
     * 内存表
     */
    private TreeMap<String, Command> index;

    /**
     * 持久化内存表
     */
    private TreeMap<String, Command> immutableIndex;

    /**
     * SsTable集合
     */
    private LinkedList<SsTable> ssTables;

    /**
     * 内存表阈值
     */
    private final long storeThreshold;

    /**
     * data block大小
     */
    private final long partSize;

    /**
     * 读写锁
     */
    private final ReentrantReadWriteLock indexLock;

    /**
     * 文件存储路径
     */
    private final String dataDir;

    /**
     * 日志文件句柄
     */
    private RandomAccessFile wal;

    /**
     * 日志文件
     */
    private File walFile;

    /**
     * 初始化
     * @param dataDir
     * @param storeThreshold
     * @param partSize
     */
    public KvStoreImpl(String dataDir, long storeThreshold, long partSize){
        try{
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
            indexLock = new ReentrantReadWriteLock();
            ssTables = new LinkedList<>();
            index = new TreeMap<>();
            immutableIndex = new TreeMap<>();
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            //目录为空，则直接初始化
            if(files == null || files.length == 0){
                walFile = new File(dataDir+WAL);
                wal = new RandomAccessFile(walFile, RW);
                return;
            }
            //从文件中恢复SsTable
            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file:files){
                String fileName = file.getName();
                if(file.isFile() && fileName.equals(WAL_TMP)){
                    restoreFromWal(new RandomAccessFile(file, RW));
                }else if(file.isFile() && fileName.endsWith(TABLE)){
                    int dotIndex = fileName.indexOf('.');
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));
                    ssTableTreeMap.put(time, SsTable.createFromFile(file.getAbsolutePath(), true));
                }else if(file.isFile() && fileName.equals(WAL)){
                    walFile = file;
                    wal = new RandomAccessFile(walFile, RW);
                    restoreFromWal(wal);
                }
            }
            ssTables.addAll(ssTableTreeMap.values());
        }catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


    }

    /**
     * 从日志文件中恢复数据到内存表
     * @param wal
     */
    public void restoreFromWal(RandomAccessFile wal){
        try {
            int start = 0;
            while(start < wal.length()){
                wal.seek(start);
                int valueLen = wal.readInt();
                byte[] bytes = new byte[valueLen];
                wal.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = ConvertUtils.toCommand(value);
                if(command != null){
                    index.put(command.getKey(), command);
                }
                start += 4;
                start += valueLen;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 添加数据
     * @param key
     * @param value
     */
    @Override
    public void set(String key, String value) {
        try {
            indexLock.writeLock().lock();
            SetCommand command = new SetCommand(key, value);
            //先写入wal日志
            byte[] bytes = JSONObject.toJSONBytes(command);
            wal.writeInt(bytes.length);
            wal.write(bytes);
            //写入内存表
            index.put(key, command);
            //内存表达到阈值，进行持久化
            if(index.size() > storeThreshold){
                switchIndex(index);
                storeToSsTable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            indexLock.writeLock().unlock();
        }

    }

    /**
     * 内存表达到阈值，转化为持久化内存表
     * @param index
     */
    public void switchIndex(TreeMap<String, Command> index){
        try {
            indexLock.writeLock().lock();
            immutableIndex = index;
            this.index = new TreeMap<>();
            wal.close();
            //切换日志
            File walTmp = new File(dataDir+WAL_TMP);
            if(walTmp.exists()){
                if(!walTmp.delete()){
                    throw new RuntimeException("删除文件失败:wal_tmp");
                }
            }
            if(!walFile.renameTo(walTmp)){
                throw new RuntimeException("重命名失败：wal");
            }
            walFile = new File(dataDir+WAL);
            wal = new RandomAccessFile(walFile, RW);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * 将持久化内存表中的数据存到SsTable
     */
    public void storeToSsTable(){
        try {SsTable ssTable = SsTable.createFromIndex(dataDir + System.currentTimeMillis() + TABLE,
                partSize, immutableIndex, true);
            ssTables.addFirst(ssTable);
            immutableIndex = null;
            File walTmp = new File(dataDir+WAL_TMP);
            if(walTmp.exists()){
                if(!walTmp.delete()){
                    throw new RuntimeException("删除文件失败:wal_tmp");
                }
            }
        } catch (Throwable e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询数据
     * @param key
     * @return
     */
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            Command command = null;
            if (index != null && index.containsKey(key)) {
                command = index.get(key);
            } else if (immutableIndex != null && immutableIndex.containsKey(key)) {
                command = immutableIndex.get(key);
            } else {
                for (SsTable ssTable : ssTables) {
                    if(ssTable == null){
                        continue;
                    }
                    command = ssTable.query(key);
                    if (command != null) {
                        break;
                    }
                }
            }
            if (command instanceof SetCommand) {
                SetCommand setCommand = (SetCommand) command;
                LoggerUtil.debug(LOGGER,"key:"+key+"-> value:"+setCommand.getValue());
                return setCommand.getValue();
            }
            if (command instanceof RmCommand) {
                LoggerUtil.debug(LOGGER,"key:"+key+"-> value:null");
                return null;
            }
            LoggerUtil.debug(LOGGER,"key:"+key+"-> value:null");
            return null;
        }catch (Throwable e){
            throw new RuntimeException(e);
        }finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * 删除数据
     * @param key
     */
    @Override
    public void rm(String key) {
        try {
            indexLock.writeLock().lock();
            RmCommand command = new RmCommand(key);
            //先写入wal日志
            byte[] bytes = JSONObject.toJSONBytes(command);
            wal.writeInt(bytes.length);
            wal.write(bytes);
            //写入内存表
            index.put(key, command);
            //内存表达到阈值，进行持久化
            if(index.size() > storeThreshold){
                switchIndex(index);
                storeToSsTable();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        wal.close();
        for(SsTable ssTable:ssTables){
            ssTable.close();
        }
    }
}
