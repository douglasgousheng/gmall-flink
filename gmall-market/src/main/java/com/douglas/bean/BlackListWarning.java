package com.douglas.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author douglas
 * @create 2020-11-27 15:11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BlackListWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;
}
