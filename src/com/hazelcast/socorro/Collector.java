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
import static com.hazelcast.socorro.Constants.*;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class Collector{
    private volatile boolean running = true;
    private static volatile int SIZE = 2500;
    private final BlockingQueue<CrashReport> generatedCrashReports = new LinkedBlockingQueue<CrashReport>(SIZE);
    Logger logger = Logger.getLogger(this.getClass().getName());


    public Collector(int nThreads) {
        final ExecutorService executors = Executors.newFixedThreadPool(nThreads);
        final IMap<Long, CrashReport> map = Hazelcast.getMap(CRASH_REPORT_MAP);
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
                if (str.startsWith("s")) {
                    SIZE = Integer.parseInt(str.substring(1));
                    logger.info("SIZE is set to " + SIZE);
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
//                int size = generatedCrashReports.size();
                int crashToGenerate = SIZE/Hazelcast.getCluster().getMembers().size();
                for (int i = 0; i < crashToGenerate; i++) {
                    CrashReport crashReport = new CrashReport();
                    crashReport.setId(Hazelcast.getIdGenerator("ids").newId());
                    crashReport.setId(random.nextInt(10000));
                    generatedCrashReports.offer(crashReport);
                }
                logger.info("Generated "+crashToGenerate + " amount of Crashreports. Current size in the local Q is: " + generatedCrashReports.size() );
            }
        }, 0, 60000);
    }

    public static void main(String[] args) {
        Collector node = new Collector(Integer.parseInt(args[0]));

    }
}
