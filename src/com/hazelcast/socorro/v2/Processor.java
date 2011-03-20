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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.socorro.CrashReport;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.hazelcast.socorro.Constants.CRASH_PROCESSED_MAP;
import static com.hazelcast.socorro.Constants.CRASH_REPORT_MAP;
import static com.hazelcast.socorro.CrashReport.*;

public class Processor {
    final IMap<Long, CrashReport> map = Hazelcast.getMap(CRASH_REPORT_MAP);
    final IMap<Long, Boolean> mapProcessed = Hazelcast.getMap(CRASH_PROCESSED_MAP);
    final ExecutorService executorService;
    final Logger logger = Logger.getLogger(this.getClass().getName());

    public Processor(int nThreads) {
        executorService = Executors.newFixedThreadPool(nThreads);
        mapProcessed.addLocalEntryListener(new EntryListener<Long, Boolean>() {
            @Override
            public void entryAdded(final EntryEvent<Long, Boolean> event) {
                final long key = event.getKey();
                processTransactional(key);
            }

            @Override
            public void entryRemoved(EntryEvent<Long, Boolean> longBooleanEntryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void entryUpdated(EntryEvent<Long, Boolean> longBooleanEntryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void entryEvicted(EntryEvent<Long, Boolean> longBooleanEntryEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                logger.info("There is "+ mapProcessed.localKeySet().size() + " number of unprocessed reports!. Processing them now!");
                for (final long key : mapProcessed.localKeySet()) {
                    processTransactional(key);
                }
            }
        }
                , 30000, 30000);
        logger.info("Processor started with " + nThreads + " threads.");
    }

    private void processTransactional(final long key) {
        executorService.execute(new Runnable() {
            public void run() {
                if (mapProcessed.containsKey(key) && mapProcessed.tryLock(key)) {
                    try {
                        CrashReport report = map.get(key);
                        if (report != null && !report.processed()) {
                            process(report.getJSON());
                            report.setProcessed(true);
                            map.put(report.getId(), report);
                            mapProcessed.remove(report.getId());
                        } else {
                            mapProcessed.remove(key);
                        }
                    } finally {
                        mapProcessed.unlock(key);
                    }
                }
            }
        });
    }

    private void process(Map<String, Object> map) {
        map.put("client_crash_date", "2011-01-24 21:15:35.0");
        map.put("dump", randomString(generate(5 * KILO_BYTE, 500 * KILO_BYTE, 50 * KILO_BYTE)));
        map.put("startedDateTime", "2011-01-24 13:15:52.657344");
        map.put("app_notes", "renderers: 0x22600,0x22600,0x20400");
        map.put("crashedThread", "0");
        map.put("cpu_info", "family 6 model 23 stepping 10 | 2");
        map.put("install_age", "134773");
        map.put("distributor", "null");
        map.put("topmost_filenames", randomString(100));
        map.put("processor_notes", randomString(100));
        map.put("user_comments", "Will test without extension.");
        map.put("build_date", "2011-01-21 15:00:00.0");
        map.put("uptime", "134771");
        map.put("uuid", "4ecc5fc9-81d5-41a4-9b4c-313942110124");
        map.put("flash_version", "[blank]");
        map.put("os_version", "10.6.5 10H574");
        map.put("distributor_version", "null");
        map.put("truncated", "true");
        map.put("process_type", "null");
        map.put("id", "211153043");
        map.put("hangid", "null");
        map.put("version", "4.0b10pre");
        map.put("build", "20110121153230");
        map.put("addons_checked", "null");
        map.put("product", "firefox");
        map.put("os_name", "Mac OS X");
        map.put("last_crash", "810473");
        map.put("date_processed", "2011-01-24 13:15:48.550858");
        map.put("cpu_name", "amd64");
        map.put("reason", "eXC_BAD_ACCESS / KERN_INVALID_ADDRESS");
        map.put("address", "0x0");
        map.put("completeddatetime", "2011-01-24 13:15:57.417639");
        map.put("signature", "nsAutoCompleteController::EnterMatch");
        Map map2 = new HashMap();
        map2.put("compatibility@addons.mozilla.org", "0.7");
        map2.put("enter.selects@agadak.net", "6");
        map2.put("{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}", "1.3.3");
        map2.put("sts-ui@sidstamm.com", "0.1");
        map2.put("masspasswordreset@johnathan.nightingale", "1.04");
        map2.put("support@lastpass.com", "1.72.0");
        map2.put("{972ce4c6-7e08-4474-a285-3208198ce6fd}", "4.0b10pre");
        map.put("addons", map2);
    }

    public static void main(String[] args) {
        Processor processor = new Processor(Integer.parseInt(args[0]));
    }
}
