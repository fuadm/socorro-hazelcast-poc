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

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CrashReport implements Serializable {
    public static final int KILO_BYTE = 1;
    Map jsonDoc;
    byte[] dump = new byte[5 * KILO_BYTE];
    private Long id;

    private static final Random random = new Random();

    public CrashReport(Map json) {
        this.jsonDoc = json;
        int size = randomSizeForBlob();
        dump = new byte[size * KILO_BYTE];
    }

    private int randomSizeForBlob() {
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

    public CrashReport() {
        this(generateMap());
    }

    public Long getId() {
        return id;
    }

    private static Map generateMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("InstallTime", new Date().getTime());
        map.put("Comments", randomString(random.nextInt(100)));
        map.put("Theme", "classic/1.0");
        map.put("Version", "4.0b10pre");
        map.put("id", "ec8030f7-c20a-464f-9b0e-13a3a9e97384");
        map.put("Vendor", "Mozilla");
        map.put("EMCheckCompatibility", random(0, 1) == 0);
        map.put("Throttleable", random.nextInt(5));
        map.put("Email", randomString(random(3, 10)) + "@" + randomString(random(5, 10)) + ".com");
        map.put("URL", "http://nighthacks.com/roller/jag/entry/the_shit_finally_hits_the");
        map.put("version", "4.0b10pre");
        map.put("CrashTime", "1295903735");
        map.put("ReleaseChannel", "nightly");
        map.put("submitted_timestamp", "2011-01-24T13:15:48.550858");
        map.put("buildid", "20110121153230");
        map.put("timestamp", 1295903748.551002);
        map.put("Notes", "Renderers: 0x22600,0x22600,0x20400");
        map.put("StartupTime", "1295768964");
        map.put("FramePoisonSize", "4096");
        map.put("FramePoisonBase", "7ffffffff0dea000");
        map.put("AdapterRendererIDs", "0x22600,0x22600,0x20400");
        map.put("Add-ons", randomString(random(200, 300)));
        map.put("BuildID", "20110121153230");
        map.put("SecondsSinceLastCrash", "810473");
        map.put("ProductName", "Firefox");
        map.put("legacy_processing", 0);
        return map;
    }

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ ";

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(AB.charAt(random.nextInt(AB.length())));
        return sb.toString();
    }

    public Map getJSON() {
        return jsonDoc;
    }

    public void setJSON(Map<String, Object> json) {
        this.jsonDoc = json;
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

    //inclusive min and max
    static int random(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    static int generate(int min, int max, int average) {
        if (random(0, 1) == 0) {
            return random(min, average);
        } else {
            return random(average, max);
        }
    }
}
