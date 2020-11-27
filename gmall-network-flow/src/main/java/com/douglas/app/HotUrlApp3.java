package com.douglas.app;

import com.douglas.bean.ApacheLog;
import com.douglas.bean.UrlViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author douglas
 * @create 2020-11-27 8:55
 */
public class HotUrlApp3 {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<ApacheLog> apachLogDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    long time = sdf.parse(fields[3]).getTime();
                    return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        OutputTag<ApacheLog> outPutTag = new OutputTag<>("sideOutPut");

        apachLogDS
                .filter(data->"GET".equals(data.getMethod()))
                .keyBy(data->data.getUrl())
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(outPutTag)
                .aggregate(new UrlCountAggFunc(),new UrlCountWindowFunc());

    }

    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator +1L;
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

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlViewCount,String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(url,window.getEnd(),input.iterator().next()));
        }
    }

}
