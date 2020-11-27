package com.douglas.app;

import com.douglas.bean.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author douglas
 * @create 2020-11-27 19:43
 */
public class LoginFailApp2 {
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

        //3.按照userId进行分组
        SingleOutputStreamOperator<String> result = loginEventDS
                .keyBy(data -> data.getUserId())
                .process(new LoginFailKeyProcessFunc());

        //4.打印
        result.print();

        //5.执行
        env.execute();


    }

    public static class LoginFailKeyProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
        private ListState<LoginEvent> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state",LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Iterator<LoginEvent> iterator = listState.get().iterator();

            if("fail".equals(value.getEventType())){
                if(iterator.hasNext()){
                    LoginEvent lastLogFail = iterator.next();
                    if(value.getTimestamp()-lastLogFail.getTimestamp()<=2){
                        //报警
                        out.collect(ctx.getCurrentKey() +
                                "在" + lastLogFail.getTimestamp() +
                                "到" + value.getTimestamp() +
                                "之间登录失败" + 2 + "次！");
                    }
                    listState.clear();
                    listState.add(value);
                }else{
                    listState.add(value);
                }
            }else {
                listState.clear();
            }


        }
    }
}

