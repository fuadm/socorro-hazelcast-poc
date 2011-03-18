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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CrashReport implements Serializable {
    public static final int KILO_BYTE = 1000;
    Map<String, Object> jsonDoc;
    byte[] dump = new byte[5 * KILO_BYTE];
    private Long id;
    private boolean processed = false;

    private static final Random random = new Random();

    public CrashReport(Map<String, Object> json, byte[] dump) {
        this.jsonDoc = json;
        this.dump = dump;
    }

    public CrashReport() {
    }

    public Long getId() {
        return id;
    }

    public Map getJSON() {
        return jsonDoc;
    }

    public byte[] getDump() {
        return dump;
    }

    public void setDump(byte[] dump) {
        this.dump = dump;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "CrashReport{" +
                "jsonDoc=" + jsonDoc +
                ", dump size =" + ((dump==null)?0:dump.length) +
                ", id=" + id +
                ", processed=" + processed +
                '}';
    }

    public static Map<String, Object> generateMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("InstallTime", new Date().toString());
        map.put("Comments", randomString(random.nextInt(100)));
        map.put("Theme", "classic/1.0");
        map.put("Version", "4.0b10pre");
        map.put("id", "ec8030f7-c20a-464f-9b0e-13a3a9e97384");
        map.put("Vendor", "Mozilla");
        map.put("EMCheckCompatibility", String.valueOf(random(0, 1) == 0));
        map.put("Throttleable", String.valueOf(random.nextInt(5)));
        map.put("Email", randomString(random(3, 10)) + "@" + randomString(random(5, 10)) + ".com");
        map.put("URL", "http://nighthacks.com/roller/jag/entry/the_shit_finally_hits_the");
        map.put("version", "4.0b10pre");
        map.put("CrashTime", "1295903735");
        map.put("ReleaseChannel", "nightly");
        map.put("submitted_timestamp", "2011-01-24T13:15:48.550858");
        map.put("buildid", "20110121153230");
        map.put("timestamp", "1295903748.551002");
        map.put("Notes", "Renderers: 0x22600,0x22600,0x20400");
        map.put("StartupTime", "1295768964");
        map.put("FramePoisonSize", "4096");
        map.put("FramePoisonBase", "7ffffffff0dea000");
        map.put("AdapterRendererIDs", "0x22600,0x22600,0x20400");
        map.put("Add-ons", randomString(random(200, 300)));
        map.put("BuildID", "20110121153230");
        map.put("SecondsSinceLastCrash", "810473");
        map.put("ProductName", "Firefox");
        map.put("legacy_processing", "0");
        return map;
    }

    public static int randomSizeForBlob() {
        if (random(0, 1) == 0) {
            return random(200, 500);
        } else {
            if (random(0, 1) == 0) {
                return random(500, 5000);
            } else {
                return random(5000, 20000);
            }
        }
    }

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ ";

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(AB.charAt(random.nextInt(AB.length())));
        return sb.toString();
    }

    //inclusive min and max
    static int random(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    public static int generate(int min, int max, int average) {
        if (random(0, 1) == 0) {
            return random(min, average);
        } else {
            return random(average, max);
        }
    }

    public boolean processed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }
}
