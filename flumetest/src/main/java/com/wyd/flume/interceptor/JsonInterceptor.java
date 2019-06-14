package com.wyd.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 自定义interceptor，
 * 实现org.apache.flume.interceptor.Interceptor接口
 * 参照SearchAndReplaceInterceptor写
 * 将程序打包上传到flume的lib目录下
 * interceptor只能跟在source后面
 *
 * a1.sources = r1
 * a1.channels = c1
 *
 * #定义sources，使用exec
 * a1.sources.r1.type = exec
 * a1.sources.r1.command = tail -F /root/log/interceptortest/access.log
 * a1.sources.r1.channels = c1
 * a1.sources.r1.interceptors = i1
 * a1.sources.r1.interceptors.i1.type = com.wyd.flume.interceptor.JsonInterceptor$JsonBuilder
 * a1.sources.r1.interceptors.i1.fields = id,name,age,fv
 * a1.sources.r1.interceptors.i1.separator = ,
 *
 * a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
 * a1.channels.c1.kafka.bootstrap.servers = node2:9092,node3:9092,node4:9092
 * a1.channels.c1.kafka.topic = c1
 * #写入kafka里的数据是文本而不是flume的event
 * a1.channels.c1.parseAsFlumeEvent = false
 */
public class JsonInterceptor implements Interceptor {

    private String[] schema;
    private String separator;

    public JsonInterceptor(String[] schema, String separator) {
        this.schema = schema;
        this.separator = separator;
    }

    public JsonInterceptor() {
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String line = new String(event.getBody(), Charsets.UTF_8);
        String[] split = line.split(separator);
        //将传入的Event中的body内容加上schema，然后在放入到event中
        LinkedHashMap<String, String> tuple = new LinkedHashMap<String, String>();
        for(int i =0; i < schema.length; i++){
            String key = schema[i];
            String value = split[i];
            tuple.put(key, value);
        }
        String json = JSONObject.toJSONString(tuple);
        event.setBody(json.getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for(Event event: events){
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    /**
     * Interceptor.Builder的生命周期方法
     * 构造器 -> configure -> build
     */
    public static class JsonBuilder implements Interceptor.Builder {

        private String fields;
        private String separator;

        @Override
        public Interceptor build() {
            String[] schema = fields.split(",");
            return new JsonInterceptor(schema, separator);
        }

        @Override
        public void configure(Context context) {
            this.fields = context.getString("fields");
            this.separator = context.getString("separator");
        }
    }


}
