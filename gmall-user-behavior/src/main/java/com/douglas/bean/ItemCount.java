package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-25 18:31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
