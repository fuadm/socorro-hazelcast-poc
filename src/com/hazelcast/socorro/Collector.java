/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.socorro;

import com.hazelcast.core.*;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import static com.hazelcast.socorro.Constants.CRASH_PROCESSED_MAP;
import static com.hazelcast.socorro.Constants.CRASH_REPORT_MAP;
import static com.hazelcast.socorro.Constants.CRASH_REPORT_Q;
import static com.hazelcast.socorro.CrashReport.*;

public class Collector {
    private volatile boolean running = true;
    private volatile int LOAD = 2*60;
    private final BlockingQueue<CrashReport> generatedCrashReports = new LinkedBlockingQueue<CrashReport>(10);
    Logger logger = Logger.getLogger(this.getClass().getName());

    public Collector(int nThreads) {
        final ExecutorService executors = Executors.newFixedThreadPool(nThreads);
        final IMap<Long, CrashReport> map = Hazelcast.getMap(CRASH_REPORT_MAP);
        final IMap<Long, Long> mapInfo = Hazelcast.getMap(CRASH_PROCESSED_MAP);
        final IQueue<Long> queue = Hazelcast.getQueue(CRASH_REPORT_Q);
        for (int i = 0; i < nThreads; i++) {
            executors.execute(new Runnable() {
                public void run() {
                    while (running) {
                        try {
                            CrashReport report = generatedCrashReports.take();
                            Transaction transaction = Hazelcast.getTransaction();
                            try {
                                transaction.begin();
                                map.put(report.getId(), report);
                                queue.offer(report.getId());
                                mapInfo.put(report.getId(), System.currentTimeMillis());
                                transaction.commit();
                            } catch (Exception e) {
                                e.printStackTrace();
                                transaction.rollback();
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            });
        }
        listenForCommands();
        generateCrashReportsPeriodically();
        logger.info("Collector started with " + nThreads + " threads.");
    }

    private void listenForCommands() {
           Hazelcast.getTopic("command").addMessageListener(new MessageListener<Object>() {
               public void onMessage(Object message) {
                   String str = (String) message;
                   logger.info("Received command: " + str);
                   if (str.startsWith("l")) {
                       LOAD = Integer.parseInt(str.substring(1));
                       logger.info("LOAD is set to " + LOAD);
                   }
               }
           });
       }


private void generateCrashReportsPeriodically() {
        final Random random = new Random();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                int k = LOAD/(Hazelcast.getCluster().getMembers().size()*60);
                for (int i = 0; i < k; i++) {
                    CrashReport crashReport = new CrashReport(CrashReport.generateMap(), new byte[randomSizeForBlob() * KILO_BYTE]);
                    crashReport.setId(Hazelcast.getIdGenerator("ids").newId());
//                    crashReport.setId(random.nextInt(10));
                    generatedCrashReports.offer(crashReport);
                }
                logger.info("Generated " + k + " number of Crash Reports. Current size in the local Q is: "+ generatedCrashReports.size());
            }
        }, 0, 1000);
    }

    public static void main(String[] args) {
        Collector node = new Collector(Integer.parseInt(args[0]));
    }
}
