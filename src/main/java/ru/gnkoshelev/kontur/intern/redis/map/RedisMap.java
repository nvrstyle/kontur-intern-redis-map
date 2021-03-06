package ru.gnkoshelev.kontur.intern.redis.map;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * @author Evgeny Lubich
 */

public class RedisMap implements Map<String,String> {

    private static final ReferenceQueue<RedisMap> QUEUE = new ReferenceQueue<>();
    private static final HashMap<String, RedisMapReference> REFERENCES = new HashMap<>();
    private static final HashMap<String, JedisSubscriber> JEDIS_SUBSCRIBERS = new HashMap<>();
    public static String host;
    public static int port;
    private static JedisPool jedisPoolGeneral;
    private static Jedis jedisGeneral;
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
        REFERENCES.put(String.valueOf(this.hashCode()), new RedisMapReference(this, QUEUE));
        JEDIS_SUBSCRIBERS.put(String.valueOf(this.hashCode()), new JedisSubscriber(jedisPool, this.hashName));
    }

    @Override
    public int size() {
        int size = 0;
        size = jedis.hlen(hashName).intValue();
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        String keyString = key.toString();
        return jedis.hexists(this.hashName, keyString);
    }

    @Override
    public boolean containsValue(Object value) {
        String valueString = value.toString();
        boolean isContain = false;
        isContain = jedis.hvals(this.hashName).stream().anyMatch(hashValue -> hashValue.equals(valueString));
        return isContain;
    }

    @Override
    public String get(Object key) {
        String keyString = key.toString();
        return jedis.hget(hashName, keyString);
    }

    @Override
    public String put(String key, String value) {
        String put = value;
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
        return put;
    }

    @Override
    public String remove(Object key) {
        String keyString = key.toString();
        if (containsKey(keyString)) {
            String previousValue = get(keyString);
            jedis.hdel(hashName, keyString);
            return previousValue;
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        jedis.hmset(hashName, (Map<String, String>) m);
    }

    @Override
    public void clear() {
        jedis.del(hashName);
    }

    @Override
    public Set<String> keySet() {
        Set<String> keyset;
        keyset = jedis.hkeys(hashName);
        return keyset;
    }

    @Override
    public Collection<String> values() {
        Collection<String> values;
        values = jedis.hvals(hashName);
        return values;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Map<String, String> hashMap = new HashMap<>();
        Map<String, String> redisMap;
        redisMap = jedis.hgetAll(hashName);
        for(String key: redisMap.keySet()) {
            hashMap.put(key, redisMap.get(key));
        }
        return hashMap.entrySet();
    }

    @Override
    public boolean equals(Object obj) {
        return this.entrySet().equals(((RedisMap) obj).entrySet());

    }



    private static void initJedisGeneral(){
        if (jedisPoolGeneral == null) {
            jedisPoolGeneral = new JedisPool(host, port);
            jedisGeneral = jedisPoolGeneral.getResource();
        }
    }

    private static int getNumSubscribers(Jedis jedis, String channel){
        int NumSubscribers = -1;
        try {
            NumSubscribers = Integer.parseInt(jedis.pubsubNumSub(channel).get(channel));
        } catch (NumberFormatException e){
            System.out.println("Exception caught in getNumSubscribers: " + e.getMessage());
        }

        return NumSubscribers;
    }

    public static int getNumSubscribers(String channel){
        initJedisGeneral();
        return getNumSubscribers(jedisGeneral, channel);
    }

    private static void clearRedisHash(Jedis jedis, String hashName) {
        jedis.del(hashName);
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
            JEDIS_SUBSCRIBERS.get(this.hashCode).unsubscribe();
            if (getNumSubscribers(jedis, this.hashName) == 0){
                clearRedisHash(jedis, this.hashName);
            }
            JEDIS_SUBSCRIBERS.remove(this.hashName);
            REFERENCES.remove(this);
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

    private static class JedisSubscriber {
        private Jedis jedisSubscriber;
        private JedisPubSub subscriber = new JedisPubSub() {};
        private Thread subscriberThread;

        public JedisSubscriber(JedisPool jedisPool, String hashName) {
            jedisSubscriber = jedisPool.getResource();
            subscribe(hashName);
        }

        public void subscribe(String channel) {
            subscriberThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        jedisSubscriber.subscribe(subscriber, channel);
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
            });
            subscriberThread.start();
        }

        public void unsubscribe(){
            subscriber.unsubscribe();
        }
    }

}
