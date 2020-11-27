package com.douglas.app;

import com.douglas.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author douglas
 * @create 2020-11-25 20:56
 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据创建流并转换为JavaBean同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            Integer.parseInt(fields[2]),
                            fields[3],
                            Long.parseLong(fields[4])
                    );
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp()*1000L;
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .sum(1);

        pv.print();

        env.execute();

    }
}
