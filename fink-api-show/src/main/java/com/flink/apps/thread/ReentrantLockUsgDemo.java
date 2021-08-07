package com.flink.apps.thread;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 重入锁的应用案例
 *
 * @author ky2009666
 * @date 2021/7/21
 **/
@Slf4j
public class ReentrantLockUsgDemo {
    /**
     * 定义列表.
     */
    private List list = new ArrayList<Integer>();

    public static void main(String[] args) {
        //定义句柄
        ReentrantLockUsgDemo reentrant = new ReentrantLockUsgDemo();
        new Thread(() -> {
            reentrant.addE(Thread.currentThread().getName());
        }, "数据线程1").start();
        new Thread(() -> {
            reentrant.addE(Thread.currentThread().getName());
        }, "数据线程2").start();
    }

    /**
     * 添加元素
     *
     * @param threadName 线程名称.
     */
    private void addE(String threadName) {
        Lock lock = new ReentrantLock();
        try {
            lock.lock();
            log.info("当前线程:" + threadName + "获取锁");
            for (int i = 0; i < 5; i++) {
                list.add(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("当前列表内容:{}", list);
            log.info("当前线程:" + threadName + "释放锁");
            lock.unlock();
        }

    }
}
