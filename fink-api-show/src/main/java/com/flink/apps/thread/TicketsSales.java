package com.flink.apps.thread;

import lombok.extern.slf4j.Slf4j;

/**
 * @author ky2009666
 * @date 2021/7/21
 **/
@Slf4j
public class TicketsSales {
    public static void main(String[] args) {
        Tickets tickets = new Tickets();
        for (int i = 0; i < 4; i++) {
            new Thread(() -> {
                try {
                    while (Boolean.TRUE){
                        tickets.sales();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, "售票员" + i).start();
        }
    }

    private static class Tickets {
        /**
         * 票数.
         */
        private int ticketsNum = 30;

        /**
         * 售票操作.
         */
        public synchronized void sales() throws InterruptedException {
            if (ticketsNum <= 0) {
                log.info("当前票已售完");
                Thread.currentThread().interrupt();
            } else {
                log.info("当前剩余票数:"+ (ticketsNum--));
            }
            Thread.sleep(200);
        }
    }
}
