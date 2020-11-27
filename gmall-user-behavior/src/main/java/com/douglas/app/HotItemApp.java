package com.douglas.app;

import com.douglas.bean.ItemCount;
import com.douglas.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author douglas
 * @create 2020-11-25 18:04
 */
public class HotItemApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp()*1000L;
                    }
                });
        //3.按照"pv"过滤,按照itemID分组,开窗,计算数据
        SingleOutputStreamOperator<ItemCount> itemCountDS = userDS
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(data -> data.getItemId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        SingleOutputStreamOperator<String> result = itemCountDS.keyBy("windowEnd").process(new TopNItemCountProcessFunc(5));

        result.print();

        env.execute();

    }
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong+1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount,Long, TimeWindow>{
        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemCount(itemId,windowEnd,count));
        }
    }
    public static class TopNItemCountProcessFunc extends KeyedProcessFunction<Tuple,ItemCount,String>{
        private Integer topSize;

        public TopNItemCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        public TopNItemCountProcessFunc() {
        }
        private ListState<ItemCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list-state",ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if(o1.getCount()>o2.getCount()){
                        return -1;
                    }else if(o1.getCount()<o2.getCount()){
                        return 1;
                    }else {
                        return 0;
                    }
                }
            });

            StringBuffer sb = new StringBuffer();
            sb.append("============================\n");
            sb.append("当前窗口结束时间为：").append(new Timestamp(timestamp-1L)).append("\n");

            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                ItemCount itemCount = itemCounts.get(i);
                sb.append("TOP").append(i+1);
                sb.append("itemId=").append(itemCount.getItemId());
                sb.append("商品热度为=").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("===================");

            listState.clear();

            Thread.sleep(1000);

            out.collect(sb.toString());

        }
    }
}
