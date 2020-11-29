package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-28 16:21
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {
    private Long orderId;
    private String eventType;
}
