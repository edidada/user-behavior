package org.example.behavior.common.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class JsonUtilsTest {

    @Test
    void roundTripsObjects() {
        Sample sample = new Sample();
        sample.setName("view");
        sample.setCount(3);

        String json = JsonUtils.toJson(sample);
        Sample parsed = JsonUtils.fromJson(json, Sample.class);

        assertEquals("view", parsed.getName());
        assertEquals(3, parsed.getCount());
    }

    @Test
    void ignoresUnknownProperties() {
        Sample parsed = JsonUtils.fromJson(
                "{\"name\":\"cart\",\"count\":1,\"unknown\":true}", Sample.class);

        assertEquals("cart", parsed.getName());
        assertEquals(1, parsed.getCount());
    }

    @Test
    void convertsToMap() {
        Map<String, Object> map = JsonUtils.toMap("{\"k\":\"v\"}");

        assertFalse(map.isEmpty());
        assertEquals("v", map.get("k"));
    }

    public static class Sample {

        private String name;

        private int count;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
