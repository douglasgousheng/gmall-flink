package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-25 19:23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {

    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;

}
