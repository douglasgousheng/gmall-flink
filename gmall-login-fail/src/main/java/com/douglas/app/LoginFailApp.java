package com.douglas.app;

import com.douglas.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author douglas
 * @create 2020-11-27 18:59
 */
public class LoginFailApp {
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
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<String> result = loginEventDS
                .keyBy(data -> data.getUserId())
                .process(new LoginFailKeyProcessFunc());

        result.print();

        env.execute();
    }

    public static class LoginFailKeyProcessFunc extends KeyedProcessFunction<Long,LoginEvent,String>{

        private ListState<LoginEvent> listState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state",LoginEvent.class));
            tsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Iterable<LoginEvent> loginEvents = listState.get();
            if(!loginEvents.iterator().hasNext()){
                if("fail".equals(value.getEventType())){
                    listState.add(value);
                    long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);

                    tsState.update(ts);
                }

            }else {
                if("fail".equals(value.getEventType())){
                    listState.add(value);
                }else{
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    listState.clear();
                    tsState.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(listState.get().iterator());
            int size = loginEvents.size();

            if(size>=2){
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(size - 1);
                out.collect(ctx.getCurrentKey()+
                        "在"+firstFail.getTimestamp()+
                        "到"+lastFail.getTimestamp()+
                        "之间登陆失败"+size+"次！");
            }

            listState.clear();
            tsState.clear();

        }
    }

}
