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

package com.hazelcast.socorro.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.socorro.CrashReport;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.htree.HTree;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBMMapStore implements MapStore<Long, CrashReport>, MapLoaderLifecycleSupport {

    volatile RecordManager recman;
    volatile HTree hashTable;
    final Logger logger = Logger.getLogger(this.getClass().getName());

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
//        String dir = (String) properties.get("dir");
        String dir = "/dev/shm";
        try {
            recman = RecordManagerFactory.createRecordManager(dir + File.separator + mapName, new Properties());
            hashTable = loadOrCreateTree(recman, mapName);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private HTree loadOrCreateTree(RecordManager recman, String name) throws IOException {
        HTree htree;
        long recid = recman.getNamedObject(name);
        if (recid != 0) {
            htree = HTree.load(recman, recid);
        } else {
            htree = HTree.createInstance(recman);
            recman.setNamedObject(name, htree.getRecid());
        }
        return htree;
    }

    @Override
    public void destroy() {
        try {
            recman.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void store(Long key, CrashReport value) {
        try {
            hashTable.put(key, value);
            recman.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeAll(Map<Long, CrashReport> crashReportMap) {
        logger.info(Thread.currentThread().getId() + ": Storing " + crashReportMap.size() + " entries ");
        long current = System.currentTimeMillis();
        try {
            for (Map.Entry<Long, CrashReport> entry : crashReportMap.entrySet()) {
                hashTable.put(entry.getKey(), entry.getValue());
            }
            recman.commit();
            logger.info(Thread.currentThread().getId() + ": Stored " + crashReportMap.size() + " entries in " + (System.currentTimeMillis() - current) + " ms");
        } catch (IOException e) {
            logger.warning(e.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Long key) {
        try {
            hashTable.remove(key);
            recman.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        try {
            for (Long key : keys) {
                hashTable.remove(key);
            }
            recman.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CrashReport load(Long key) {
        try {
            return (CrashReport) hashTable.get(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Long, CrashReport> loadAll(Collection<Long> keys) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
