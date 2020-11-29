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
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long timestamp;
}
