package utils;

import com.alibaba.fastjson.JSONObject;
import model.command.Command;
import model.command.CommandTypeEnum;
import model.command.RmCommand;
import model.command.SetCommand;

public class ConvertUtils {
    public static final String TYPE = "type";

    public static Command toCommand(JSONObject value){
        if(value.getString(TYPE).equals(CommandTypeEnum.SET.name())){
            return value.toJavaObject(SetCommand.class);
        }else if(value.getString(TYPE).equals(CommandTypeEnum.RM.name())){
            return value.toJavaObject(RmCommand.class);
        }
        return null;
    }
}
