package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-25 19:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlViewCount {

    private String url;
    private Long windowEnd;
    private Long count;

}
