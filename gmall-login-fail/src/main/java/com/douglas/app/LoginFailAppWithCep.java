package com.douglas.app;

import com.douglas.bean.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


/**
 * @author douglas
 * @create 2020-11-28 10:23
 */
public class LoginFailAppWithCep {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,提取时间戳
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.定义序列模式
//        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        })
                .times(2)
                .consecutive()
                .within(Time.seconds(5));

        //4.将序列模式作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventDS.keyBy(data -> data.getUserId()), pattern);

        //5.提取事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailSelectFunc());

        result.print();
        env.execute();
    }
    public static class LoginFailSelectFunc implements PatternSelectFunction<LoginEvent,String>{

        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            List<LoginEvent> start = pattern.get("start");
//            List<LoginEvent> next = pattern.get("next");
            return "在" + start.get(0).getTimestamp() +
                    "到" + start.get(1).getTimestamp() + " " +
                    start.get(0).getUserId() +
                    "之间登录失败2次！";
        }
    }
}
