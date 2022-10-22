package model.ssTable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import model.Position;
import model.command.Command;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import utils.ConvertUtils;
import utils.LoggerUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 排序字符串表
 */
public class SsTable implements Closeable {
    public static final String RW = "rw";

    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    /**
     * 表索引信息
     */
    private TableMetaInfo tableMetaInfo;

    /**
     * 稀疏索引
     */
    private TreeMap<String, Position> sparseIndex;

    /**
     * 文件句柄
     */
    private final RandomAccessFile tableFile;

    /**
     * 文件路径
     */
    private final String filePath;

    /**
     * data block是否压缩
     */
    private boolean enablePartDataCompressed;

    /**
     * @param filePath  文件路径
     * @param partSize  data block的大小
     * @param enablePartDataCompressed 是否压缩
     */
    public SsTable(String filePath, long partSize, boolean enablePartDataCompressed){
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setPartSize(partSize);
        this.filePath = filePath;
        sparseIndex = new TreeMap<String, Position>();
        this.enablePartDataCompressed = enablePartDataCompressed;
        try {
            tableFile = new RandomAccessFile(filePath, RW);
            tableFile.seek(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从内存中构建SsTable
     * @param filePath
     * @param partSize
     * @param enablePartDataCompressed
     * @param index
     * @return
     */
    public static SsTable createFromIndex(String filePath, long partSize,
                                   TreeMap<String, Command> index, boolean enablePartDataCompressed){
        SsTable ssTable = new SsTable(filePath, partSize, enablePartDataCompressed);
        ssTable.initFromIndex(index);
        return ssTable;
    }

    /**
     * 从文件中构建SsTable
     * @param filePath
     * @param enablePartDataCompressed
     * @return
     */
    public static SsTable createFromFile(String filePath, boolean enablePartDataCompressed){
        SsTable ssTable = new SsTable(filePath, 0, enablePartDataCompressed);
        ssTable.restoreFromFile();
        return ssTable;
    }

    /**
     * 从内存转化为SsTable
     * @param index
     */
    public void initFromIndex(TreeMap<String, Command> index){
        try {
            JSONObject partData = new JSONObject(true);
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Command command : index.values()){
                if(command instanceof SetCommand){
                    SetCommand set = (SetCommand) command;
                    partData.put(set.getKey(), set);
                }
                if(command instanceof RmCommand){
                    RmCommand rm = (RmCommand) command;
                    partData.put(rm.getKey(), rm);
                }
                // 开始写入数据段
                if(partData.size() >= tableMetaInfo.getPartSize()){
                    writeDataPart(partData);
                }
            }
            //将剩余的数据也写入文件
            if(partData.size() > 0){
                writeDataPart(partData);
            }

            tableMetaInfo.setDataLen(tableFile.getFilePointer()-tableMetaInfo.getDataStart());
            byte[] sparseIndexByte = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            tableMetaInfo.setIndexLen(sparseIndexByte.length);
            tableFile.write(sparseIndexByte);
            LoggerUtil.debug(LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info(LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 把数据分区写入文件
     * @param partData
     */
    public void writeDataPart(JSONObject partData) throws IOException {
        LoggerUtil.debug(LOGGER,"write beginning...");
        byte[] partDataByte = partData.toJSONString().getBytes(StandardCharsets.UTF_8);

        //partData进行压缩
        if(enablePartDataCompressed){
            LoggerUtil.debug(LOGGER, "writeDataPart, partDataByte size:["+partDataByte.length+"]");
            partDataByte = Snappy.compress(partDataByte);
            LoggerUtil.debug(LOGGER, "writeDataPart, partDataByte compressed size:["+partDataByte.length+"]");
        }

        //partData写入文件
        long start = tableFile.getFilePointer();
        LoggerUtil.debug(LOGGER, "writeDataPart, before write start:["+start+"]");
        tableFile.write(partDataByte);
        LoggerUtil.debug(LOGGER, "writeDataPart, after write start:["+tableFile.getFilePointer()+"]");

        //将每个partData的第一个key写入稀疏索引
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        byte[] finalPartDataByte = partDataByte;
        firstKey.ifPresent(k-> sparseIndex.put(k, new Position(start, finalPartDataByte.length)));

        partData.clear();
    }

    public void restoreFromFile(){
        try {
            tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][tableMetaInfo]: {}", tableMetaInfo);

            byte[] sparseIndexByte = new byte[(int)tableMetaInfo.getIndexLen()];
            tableFile.seek(tableMetaInfo.getIndexStart());
            tableFile.read(sparseIndexByte);
            String sparseIndexStr = new String(sparseIndexByte, StandardCharsets.UTF_8);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseIndexStr]: {}", sparseIndexStr);
            sparseIndex = JSONObject.parseObject(sparseIndexStr,
                    new TypeReference<TreeMap<String, Position>>(){});
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseIndex]: {}", sparseIndex);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Command query(String key){
        try {
            List<Position> positionList = new LinkedList<>();
            for(String k:sparseIndex.keySet()){
                if(k.compareTo(key) <= 0){
                    positionList.add(sparseIndex.get(k));
                }else {
                    break;
                }
            }
            //拿到数据的稀疏索引
            if(positionList.size() == 0){
                return null;
            }
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][positionList]: {}", positionList);
            Position position = positionList.get(positionList.size()-1);
            //找到key所在的data block中的数据
            tableFile.seek(position.getStart());
            byte[] data = new byte[(int) position.getLength()];
            tableFile.read(data);
            //解压缩
            if(enablePartDataCompressed){
                data = Snappy.uncompress(data);
            }
            JSONObject dataBlock = JSONObject.parseObject(new String(data, StandardCharsets.UTF_8));
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][dataBlock]: {}", dataBlock);
            if(dataBlock.containsKey(key)){
                JSONObject value = dataBlock.getJSONObject(key);
                return ConvertUtils.toCommand(value);
            }
            return null;
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }
}
