package utils;

import org.slf4j.Logger;

/**
 * 日志工具类
 */
public class LoggerUtil {
    public static void debug(Logger logger, String format, Object... args){
        if(logger.isDebugEnabled()){
            logger.debug(format, args);
        }
    }
    public static void info(Logger logger, String format, Object... args){
        if(logger.isInfoEnabled()){
            logger.info(format, args);
        }
    }
    public static void error(Logger logger, String format, Object... args){
        if(logger.isErrorEnabled()){
            logger.error(format, args);
        }
    }
}
