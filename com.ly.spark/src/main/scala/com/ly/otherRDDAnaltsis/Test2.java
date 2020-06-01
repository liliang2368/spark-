package com.ly.otherRDDAnaltsis;

import java.util.concurrent.*;

public class Test2 {
    public static void main(String[] args) {
       // ExecutorService pool= Executors.newSingleThreadExecutor();
      //  ExecutorService pool= Executors.newFixedThreadPool(4);
//        ExecutorService pool= Executors.newCachedThreadPool();

//        for(int i=0;i<10;i++){
//            //execte提交任务
//            pool.execute(new Runnable() {
//                @Override
//                public void run() {
//                    System.out.println(Thread.currentThread().getName());
//                    try {
//                        Thread.sleep(2000);
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
    //线程调度器
        ScheduledExecutorService pool= Executors.newScheduledThreadPool(5);
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("每隔3秒");

            }
        },1,3, TimeUnit.SECONDS);
    }
}
