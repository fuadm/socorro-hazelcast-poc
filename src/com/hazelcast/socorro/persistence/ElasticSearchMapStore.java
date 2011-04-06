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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class ElasticSearchMapStore implements MapStore<Long, CrashReport>, MapLoaderLifecycleSupport {
    private static final String TYPE = "crash_report";
    private static final String DUMP = "blob_dump";
    volatile TransportClient client;
    volatile String INDEX;

    final Logger logger = Logger.getLogger(this.getClass().getName());

    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.INDEX = mapName.toLowerCase();
        this.client = new TransportClient();
        for (int i = 0; i < 10; i++) {
            String property = "host" + i;
            properties.get(property);
            if (properties.get(property) != null) {
                String value = (String) properties.get(property);
                this.client.addTransportAddress(new InetSocketTransportAddress(value, 9300));
            }
        }
    }

    public void destroy() {
        client.close();
    }

    public void store(Long key, CrashReport value) {
        ListenableActionFuture<IndexResponse> result = client.prepareIndex(INDEX, TYPE, String.valueOf(key)).setSource(value.getJSON()).execute();
        verify(result);
    }

    private void verify(ListenableActionFuture<? extends ActionResponse> result) {
        try {
            result.get();
        } catch (InterruptedException e) {
            return;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void storeAll(Map<Long, CrashReport> crashReportMap) {

        logger.info(Thread.currentThread().getId() + ": Storing " + crashReportMap.size() + " entries ");
        long current = System.currentTimeMillis();
        BulkRequest bulkRequest = new BulkRequest();
        for (Long key : crashReportMap.keySet()) {
            Map map = new HashMap(crashReportMap.get(key).getJSON());
            map.put(DUMP, crashReportMap.get(key).getDump());
            bulkRequest.add(new IndexRequest(INDEX, TYPE, String.valueOf(key)).source(map));
        }
        verify(bulkRequest);
        logger.info(Thread.currentThread().getId() + ": Stored " + crashReportMap.size() + " entries in " + (System.currentTimeMillis() - current) + " ms");
    }

    private void verify(BulkRequest bulkRequest) {
        try {
            BulkResponse response = client.bulk(bulkRequest).get();
            if (response.hasFailures()) {
                throw new RuntimeException("There was a failure while indexing entries to ES: " + response);
            }
        } catch (InterruptedException e) {
            return;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void delete(java.lang.Long key) {
        ListenableActionFuture<DeleteResponse> result = client.prepareDelete(INDEX, TYPE, String.valueOf(key)).execute();
        verify(result);
    }

    public void deleteAll(Collection<java.lang.Long> keys) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Long key : keys) {
            bulkRequest.add(new DeleteRequest(INDEX, TYPE, String.valueOf(key)));
        }
        verify(bulkRequest);
    }

    public CrashReport load(java.lang.Long key) {
        ListenableActionFuture<GetResponse> result = client.prepareGet(INDEX, TYPE, String.valueOf(key)).execute();
        try {
            GetResponse response = result.get();
            Map map = response.getSource();
            byte[] dump = (byte[]) map.remove(DUMP);
            CrashReport crashReport = new CrashReport(map, dump);
            crashReport.setDump(dump);
            return crashReport;
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<java.lang.Long, CrashReport> loadAll(Collection<java.lang.Long> keys) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
