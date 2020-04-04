package ru.gnkoshelev.kontur.intern.redis.map;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Gregory Koshelev
 */
public class RedisMapTest {
    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap("docker",6379);
        Map<String, String> map2 = new RedisMap("docker",6379);

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));
    }

    @Test
    public void GarbageCollectorTests() throws InterruptedException {
        Map<String, String> map1 = new RedisMap("docker",6379);
        String mapHashName = ((RedisMap) map1).getHashName();
        Map<String, String> map2 = new RedisMap(mapHashName, "docker",6379);
        map1.put("1", "Один");
        map1.put("2", "Два");
        map1.put("3", "Три");
        map1.put("4", "Четыре");
        Assert.assertEquals(4, map1.size());
        Assert.assertEquals(4, map2.size());
        map1 = null;
        System.gc();
        Thread.sleep(500);
        Assert.assertEquals(0, map2.size());
    }

}
