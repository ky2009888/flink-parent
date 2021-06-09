package com.flink.apps.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author ky2009666
 * @description 单词统计类
 * @date 2021/6/6
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public  class WordInfo implements Serializable {
    /**
     * 单词内容.
     */
    private String wordStr;
    /**
     * 单词次数.
     */
    private long countWords;
}
