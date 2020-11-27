package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-25 17:55
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PvCount {
    private String pv;
    private Long windowEnd;
    private Long count;
}
