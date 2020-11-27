package com.douglas.app;

import com.douglas.bean.AdClickEvent;
import com.douglas.bean.AdCountByProvince;
import com.douglas.bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * @author douglas
 * @create 2020-11-27 15:10
 */
public class AdClickByProvinceApp {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流转换为JavaBean,并指定时间戳字段
        SingleOutputStreamOperator<AdClickEvent> adClickDS = env.readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            fields[2],
                            fields[3],
                            Long.parseLong(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdClickEvent> filterByClickCount = adClickDS
                .keyBy("userId", "adId")
                .process(new AdClickProcessFunc(100L));

        //4.按照省份分组,开窗,计算各个省份广告点击总数
        SingleOutputStreamOperator<AdCountByProvince> result = filterByClickCount
                .keyBy(data -> data.getProvince())
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdClickAgg(), new AdCountWindowFunc());

        DataStream<BlackListWarning> sideOutput = filterByClickCount.getSideOutput(new OutputTag<BlackListWarning>("outPut") {
        });

        result.print();
        sideOutput.print("sideOutput");
        env.execute();

    }

    public static class AdClickAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class AdCountWindowFunc implements WindowFunction<Long, AdCountByProvince,String, TimeWindow>{


        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountByProvince(s,windowEnd,count));
        }
    }


    public static class AdClickProcessFunc extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

        private Long maxClick;

        public AdClickProcessFunc() {
        }

        public AdClickProcessFunc(Long maxClick) {
            this.maxClick = maxClick;
        }

        private ValueState<Long> countState;
        private ValueState<Boolean> isBlackList;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            isBlackList = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isblack-state", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            Long count = countState.value();

            if (count == null) {
                countState.update(1L);
                long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (60 * 60 * 24 * 1000L) - (8 * 60 * 60 * 1000L);
                System.out.println(new Timestamp(ts));
                ctx.timerService().registerEventTimeTimer(ts);


            } else {
                long curClickCount = count + 1L;
                countState.update(curClickCount);
                if (curClickCount >= maxClick) {
                    if (isBlackList.value() == null){
                        ctx.output(new OutputTag<BlackListWarning>("outPut"){}
                        ,new BlackListWarning(value.getUserId(),value.getUserId(),"点击次数超过"+maxClick+"次!"));
                        isBlackList.update(true);
                    }
                    return;
                }
            }

            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isBlackList.clear();
        }
    }
}
