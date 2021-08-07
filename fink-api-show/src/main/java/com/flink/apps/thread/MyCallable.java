package com.flink.apps.thread;

import java.util.concurrent.Callable;

/**
 * 多线程返回
 *
 * @author ky2009666
 * @date 2021/7/21
 **/
public class MyCallable {
    public static void main(String[] args) {

    }
    public class CallableGet implements Callable{
        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Integer call() throws Exception {
            return 0;
        }
    }
}
