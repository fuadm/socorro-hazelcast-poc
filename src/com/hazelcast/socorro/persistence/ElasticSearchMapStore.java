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
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ElasticSearchMapStore implements MapStore<Long, CrashReport>, MapLoaderLifecycleSupport {
    private static final String TYPE = "crash_report";
    volatile Client client;
    volatile String INDEX;

    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.INDEX = mapName;
        String host1 = (String) properties.get("host1");
        String host2 = (String) properties.get("host2");
        this.client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress(host1, 9300))
                .addTransportAddress(new InetSocketTransportAddress(host2, 9300));
    }

    public void destroy() {
        client.close();
    }

    public void store(java.lang.Long key, CrashReport value) {
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

    public void storeAll(Map<java.lang.Long, CrashReport> longCrashReportMap) {
        BulkRequest bulkRequest = new BulkRequest();
        for (Long key : longCrashReportMap.keySet()) {
            bulkRequest.add(new IndexRequest(INDEX, TYPE, String.valueOf(key)).source(longCrashReportMap.get(key).getJSON()));
        }
        verify(bulkRequest);
    }

    private void verify(BulkRequest bulkRequest) {
        try {
            BulkResponse response = client.bulk(bulkRequest).get();
            if (response.hasFailures()) {
                throw new RuntimeException("There was a failure while indexing entries to ES");
            }
        } catch (InterruptedException e) {
            return;
        } catch (ExecutionException e) {
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
            CrashReport crashReport = new CrashReport(response.getSource());
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
