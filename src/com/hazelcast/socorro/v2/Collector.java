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

package com.hazelcast.socorro.v2;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Transaction;
import com.hazelcast.socorro.CrashReport;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hazelcast.socorro.Constants.CRASH_PROCESSED_MAP;
import static com.hazelcast.socorro.Constants.CRASH_REPORT_MAP;
import static com.hazelcast.socorro.CrashReport.KILO_BYTE;
import static com.hazelcast.socorro.CrashReport.randomSizeForBlob;

public class Collector {
    private volatile boolean running = true;
    private static volatile int SIZE = 1;
    private final BlockingQueue<CrashReport> generatedCrashReports = new LinkedBlockingQueue<CrashReport>(1000);
    Logger logger = Logger.getLogger(this.getClass().getName());

    public Collector(int nThreads) {
        final ExecutorService executors = Executors.newFixedThreadPool(nThreads);
        final IMap<Long, CrashReport> map = Hazelcast.getMap(CRASH_REPORT_MAP);
        final IMap<Long, Boolean> mapProcessed = Hazelcast.getMap(CRASH_PROCESSED_MAP);
        for (int i = 0; i < nThreads; i++) {
            executors.execute(new Runnable() {
                public void run() {
                    while (running) {
                        Transaction txn = Hazelcast.getTransaction();
                        try {
                            txn.begin();
                            CrashReport report = generatedCrashReports.take();
                            map.put(report.getId(), report);
                            mapProcessed.put(report.getId(), true);
                            txn.commit();
                        } catch (Throwable e) {
                            logger.log(Level.INFO, "rollbacking", e);
                            txn.rollback();
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
                for (int i = 0; i < SIZE; i++) {
                    CrashReport crashReport = new CrashReport(CrashReport.generateMap(), new byte[randomSizeForBlob() * KILO_BYTE]);
                    crashReport.setId(Hazelcast.getIdGenerator("ids").newId());
//                    crashReport.setId(random.nextInt(10));
                    generatedCrashReports.offer(crashReport);
                }
                logger.info("Generated " + SIZE + " amount of Crashreports. Current size in the");
            }
        }, 0, 1000);
    }

    public static void main(String[] args) {
        Collector node = new Collector(Integer.parseInt(args[0]));
    }
}
