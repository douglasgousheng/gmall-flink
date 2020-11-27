package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-27 15:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChannelBehaviorCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;
}
