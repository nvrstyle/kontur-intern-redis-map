package ru.gnkoshelev.kontur.intern.redis.map;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author Evgeny Lubich
 */

public class RedisMap implements Map<String,String> {

    private static final ReferenceQueue<RedisMap> queue = new ReferenceQueue<>();
    private static final HashMap<String, RedisMapReference> references = new HashMap<>();
    private static int count = 0;

    private JedisPool jedisPool;
    private Jedis jedis;
    private String hashName;
    public String getHashName() {
        return hashName;
    }

    public void setHashName(String hashName) {
        this.hashName = hashName;
    }

    public RedisMap(){
        this("", "localhost", 6379);

    }

    public RedisMap(String hashName) {
        this(hashName,"localhost", 6379);
    }

    public RedisMap(String host, int port){
        this("", host, port);
    }

    public RedisMap(String hashName, String host, int port){
        jedisPool = new JedisPool(host, port);
        jedis = jedisPool.getResource();
        if (hashName.equals("")){
            this.hashName = Integer.toHexString(hashCode());
        } else {
            this.hashName = hashName;
        }
        count++;
        references.put(String.valueOf(this.hashCode()), new RedisMapReference(this, queue));
    }

    @Override
    public int size() {
        int size = 0;
        try {
            size = jedis.hlen(hashName).intValue();
        } catch (Exception e){
            System.out.println("Exception caught in size: " + e.getMessage());
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        String keyString = key.toString();
        try{
            return jedis.hexists(this.hashName, keyString);
        } catch (Exception e){
            System.out.println("Exception caught in containsKey: " + e.getMessage());
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        String valueString = value.toString();
        boolean isContain = false;
        try{
            isContain = jedis.hvals(this.hashName).stream().anyMatch(hashValue -> hashValue.equals(valueString));
        } catch (Exception e){
            System.out.println("Exception caught in containsValue: " + e.getMessage());
        }
        return isContain;
    }

    @Override
    public String get(Object key) {
        String keyString = key.toString();
        try {
            return jedis.hget(hashName, keyString);
        } catch (Exception e){
            System.out.println("Exception in get: " + e.getMessage());
        }
        return null;
    }

    @Override
    public String put(String key, String value) {
        String put = value;
        try {
            if (!containsKey(key)) {
                jedis.hset(hashName, key, value);
                return null;
            }
            for (String hkey : jedis.hkeys(hashName))
                if (hkey.equals(key)) {
                    put = jedis.hget(hashName, hkey);
                    jedis.hset(hashName, key, value);
                    return put;
                }
        } catch (Exception e){
            System.out.println("Exception caught in put: " + e.getMessage());
        }
        return put;
    }

    @Override
    public String remove(Object key) {
        String keyString = key.toString();
        try {
            if (containsKey(keyString)) {
                String previousValue = get(keyString);
                jedis.hdel(hashName, keyString);
                return previousValue;
            }
        } catch (Exception e){
            System.out.println("Exception caught in remove: " + e.getMessage());
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        try{
            jedis.hmset(hashName, (Map<String, String>) m);
        } catch (Exception e){
            System.out.println("Exception caught in containsKey: " + e.getMessage());
        }
    }

    @Override
    public void clear() {
        try{
            jedis.del(hashName);
        } catch (Exception e){
            System.out.println("Exception caught in containsKey: " + e.getMessage());
        }
    }

    @Override
    public Set<String> keySet() {
        Set<String> keyset;
        try{
            keyset = jedis.hkeys(hashName);
            return keyset;
        } catch (Exception e){
            System.out.println("Exception caught in keySet: " + e.getMessage());
        }

        keyset = new HashSet<>();
        return keyset;
    }

    @Override
    public Collection<String> values() {
        Collection<String> values;
        try{
            values = jedis.hvals(hashName);
            return values;
        } catch (Exception e){
            System.out.println("Exception caught in values: " + e.getMessage());
        }

        values = new ArrayList<>();
        return values;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Map<String, String> hashMap = new HashMap<>();
        Map<String, String> redisMap;
        try {
            redisMap = jedis.hgetAll(hashName);
            for(String key: redisMap.keySet()) {
                hashMap.put(key, redisMap.get(key));
            }
            return hashMap.entrySet();
        } catch (Exception e){
            System.out.println("Exception caught in entrySet: " + e.getMessage());
        }

        return hashMap.entrySet();
    }

    @Override
    public boolean equals(Object obj) {
        return this.entrySet().equals(((RedisMap) obj).entrySet());
    }

    private static void clearRedisHash(Jedis jedis, String hashName) {
        try {
            jedis.del(hashName);
        } catch (Exception e) {
            System.out.println("Exception caught in clearRedisHash: " + e.getMessage());
        }
    }


    private static class RedisMapReference extends PhantomReference<RedisMap> {
        private String hashCode;
        private String hashName;
        private JedisPool jedisPool;
        private Jedis jedis;

        public RedisMapReference(RedisMap redisMap, ReferenceQueue<RedisMap> queue) {
            super(redisMap, queue);
            hashName = redisMap.getHashName();
            hashCode = String.valueOf(redisMap.hashCode());
            jedisPool = redisMap.jedisPool;
            jedis = redisMap.jedis;
            Thread thread = new QueueReadingThread(queue);
            thread.start();
        }

        public void clearRedis() {
            clearRedisHash(jedis, this.hashName);
            references.remove(this);
            clear();
        }

        private static class QueueReadingThread extends Thread {

            private ReferenceQueue<RedisMap> referenceQueue;

            public QueueReadingThread(ReferenceQueue<RedisMap> referenceQueue) {
                this.referenceQueue = referenceQueue;
            }

            @Override
            public void run() {
                RedisMapReference reference = null;

                //ждем, пока в очереди появятся ссылки
                while (reference == null) {

                    try {
                        Thread.sleep(50);
                        reference = (RedisMapReference) referenceQueue.poll();
                    }

                    catch (InterruptedException e) {
                        throw new RuntimeException("Поток " + getName() + " был прерван!");
                    }
                }
                //Освобождение ресурсов перед удалением ссылки
                reference.clearRedis();
            }
        }
    }

}
