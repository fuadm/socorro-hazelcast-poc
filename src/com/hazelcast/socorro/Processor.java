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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Transaction;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.hazelcast.socorro.Constants.CRASH_REPORT_MAP;
import static com.hazelcast.socorro.Constants.CRASH_REPORT_Q;

public class Processor {
    private volatile boolean running = true;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public Processor(int nThreads) {
        final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        final IMap<Long, CrashReport> map = Hazelcast.getMap(CRASH_REPORT_MAP);
        final IQueue<Long> queue = Hazelcast.getQueue(CRASH_REPORT_Q);
        for (int i = 0; i < nThreads; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    while (running) {
                        Transaction transaction = Hazelcast.getTransaction();
                        try {
                            transaction.begin();
                            Long reportId = queue.take();
                            CrashReport report = map.get(reportId);
                            process(report.getJSON());
                            map.put(reportId, report);
                            transaction.commit();
                        } catch (Exception e) {
                            transaction.rollback();
                        }
                    }
                }
            });
        }
        logger.info("Processor started with " + nThreads + " threads.");
    }

    private void process(Map json) {

    }

    public static void main(String[] args) {
        Processor processor = new Processor(Integer.parseInt(args[0]));
    }
}
