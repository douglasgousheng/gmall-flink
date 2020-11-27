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
public class AdCountByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
