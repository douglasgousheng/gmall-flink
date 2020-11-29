package com.douglas.app;

import com.douglas.bean.OrderEvent;
import com.douglas.bean.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * @author douglas
 * @create 2020-11-28 22:37
 */
public class OrderTimeoutAppWithCep {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件数据创建流,转换为JavaBean,提取事件时间
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventDS.keyBy(data -> data.getOrderId()), pattern);

        SingleOutputStreamOperator<OrderResult> result = patternStream.select(new OutputTag<OrderResult>("timeout") {
        }, new TimeOutSelectFunc(), new SelectFunc());

        result.print("payed");
        result.getSideOutput(new OutputTag<OrderResult>("timeout"){}).print("timeout");

        env.execute();
    }

    public static class TimeOutSelectFunc implements PatternTimeoutFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            List<OrderEvent> start = pattern.get("start");
            return new OrderResult(start.get(0).getOrderId(),"timeout"+timeoutTimestamp);
        }
    }

    public static class SelectFunc implements PatternSelectFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            List<OrderEvent> start = pattern.get("start");
            return new OrderResult(start.get(0).getOrderId(),"payed");
        }
    }
}
