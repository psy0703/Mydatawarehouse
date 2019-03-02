package com.ng.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume日志过滤工具类
 */
public class LogUtils {

    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);
    /**
     * 日志检查，正常的log会返回true，错误的log返回false
     *
     * @param log
     */
    public static boolean validataReportLog(String log) {
        /*
        1、服务器校验的规则：13位，全部是数字
        2、json规则：开头结尾是“{  }”
         */
        String[] logArray = log.split("\\|");

        try {


            if (logArray.length < 2) {
                return false;
            }
            //检查第一串是否为时间戳 或者 不是全部是数字
            if (logArray[0].length() != 13 || !NumberUtils.isDigits(logArray[0])) {
                return false;
            }

            //检查第二串是否为正确的json，这里粗略检查，有时候我们需要从后面json传入错的数据做分析
            if (!logArray[1].trim().startsWith("{") || !logArray[1].trim().endsWith("}")) {
                return false;
            }
        } catch (Exception e){
           //错误日志的打印，需要查看
            logger.error("parse error,message is " + log);
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }
}
