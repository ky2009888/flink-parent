package com.flink.apps.hot.product;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * list数据大小比较
 *
 * @author ky2009666
 * @date 2021/7/11
 **/
@Slf4j(topic = "list数字比较大小")
public class CompareList {
    public static void main(String[] args) {
        List<Integer> numList = Arrays.asList(5, 3, 2, 4, 8, 2, 6);
        List<Integer> collect = numList.stream().sorted().collect(Collectors.toList());
        log.info("{}", collect);
        List<Integer> collect1 = numList.stream().sorted(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.intValue() - o2.intValue() > 0 ? -1 : 1;
            }
        }).collect(Collectors.toList());
        log.info("{}", collect1);
    }
}
