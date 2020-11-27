package com.douglas.app;

import com.douglas.bean.UserBehavior;
import com.douglas.bean.UvCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

/**
 * @author douglas
 * @create 2020-11-27 20:05
 */
public class UvCountWithBloomFilterApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据创建流并转换为JavaBean同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] fileds = line.split(",");
                    return new UserBehavior(Long.parseLong(fileds[0]),
                            Long.parseLong(fileds[1]),
                            Integer.parseInt(fileds[2]),
                            fileds[3],
                            Long.parseLong(fileds[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UvCount> result = userDS
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvWithBloomFilterWindowFunc());

        result.print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<UserBehavior,TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UvWithBloomFilterWindowFunc extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        Jedis jedis;

        MyBloomFilter myBloomFilter;

        String uvCountRedisKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis=new Jedis("hadoop102",6379);
            myBloomFilter=new MyBloomFilter(1L<<29);
            uvCountRedisKey="UvCount";
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {

            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            String bitMapRedisKey = "UvBitMap" + windowEnd;
            Long offset = myBloomFilter.hash(elements.iterator().next().getUserId() + "");
            Boolean exist = jedis.getbit(bitMapRedisKey, offset);
            if(!exist){
                jedis.hincrBy(uvCountRedisKey,windowEnd,1);
                jedis.setbit(bitMapRedisKey,offset,true);
            }
            long count = Long.parseLong(jedis.hget(uvCountRedisKey, windowEnd));

            out.collect(new UvCount("uv",windowEnd,count));
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
    public static class MyBloomFilter{
        private Long cap;

        public MyBloomFilter(Long cap) {
            this.cap = cap;
        }

        public Long hash(String value){
            int result =0;
            for (char c : value.toCharArray()) {
                result = result * 31 + c;
            }
            return result & (cap -1);
        }
    }



}
