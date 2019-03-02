package com.ng.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flume日志类型区分拦截器LogTypeInterceptor
 */
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1、获取flume接收的消息头
        Map<String, String> headers = event.getHeaders();
        //2、获取flume接受的json数据数组
        byte[] body = event.getBody();
        String logType = "";
        //3、将获取到的json数据转化 为字符串
        String jsonStr = new String(body);
        //startLog
        if (jsonStr.contains("start")) {
            logType = "start";
        } else {
            //eventLog
            logType = "event";
        }
        //3、将日志类型存储带flume的Header中，供拦截器使用
        headers.put("logType", logType);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event : events) {
            Event interceptEvent = intercept(event);
            interceptors.add(interceptEvent);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
