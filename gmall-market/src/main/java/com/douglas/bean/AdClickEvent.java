package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-27 15:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}