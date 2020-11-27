package com.douglas.app;

import com.douglas.bean.ChannelBehaviorCount;
import com.douglas.bean.MarketUserBehavior;
import com.douglas.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author douglas
 * @create 2020-11-27 18:13
 */
public class MarketByChannelApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<MarketUserBehavior> marketUserDS = env.addSource(new MarketBehaviorSource());
        SingleOutputStreamOperator<ChannelBehaviorCount> result = marketUserDS
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketAggFunc(), new MarketWindowFunc());

        result.print();

        env.execute();
    }


    public static class MarketWindowFunc implements WindowFunction<Long, ChannelBehaviorCount, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new ChannelBehaviorCount(tuple.getField(0).toString(),tuple.getField(1).toString(),windowEnd,count));

        }
    }


    public static class MarketAggFunc implements AggregateFunction<MarketUserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
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


}
