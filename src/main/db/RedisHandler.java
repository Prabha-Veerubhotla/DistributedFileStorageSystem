package main.db;
import com.sun.istack.NotNull;
import main.entities.FileEntity;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;


public class RedisHandler {
    Logger logger = Logger.getLogger(RedisHandler.class.getName());
    private Jedis redisConnector;
    public static JedisPool redisPool;
    public static final int MAX_POOL_SIZE = 100;
    public static final String HOST_NAME = "localhost";

    public synchronized Jedis getPoolConnection() {
        logger.info("Setting Redis Pool");
        if (redisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(MAX_POOL_SIZE);
            redisPool = new JedisPool(jedisPoolConfig, HOST_NAME);
        }
        return redisPool.getResource();
    }

    /**
     * Method to initialize Jedis instance
     */
    public RedisHandler() {
        logger.info("Getting pool connection");
        redisConnector = getPoolConnection();
    }

    /**
     * Converts object to bytes
     * @return byte[]
     */
    public byte[] serialize(Object obj) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oout = new ObjectOutputStream(bout);
            oout.writeObject(obj);
        } catch (IOException ex) {
            logger.warning("Error in serializing data " + obj.getClass());
        }
        return bout.toByteArray();
    }

    /**
     *
     */
    public Object deserialize(byte[] bytes) {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        Object out = null;
        try {
            ObjectInputStream oin = new ObjectInputStream(bin);
            out = oin.readObject();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return out;
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public String put(@NotNull String userName, @NotNull String fileName, String seqID, byte[] content) {
        byte[] userNameByte = serialize(userName);
        try {
            if (redisConnector.exists(userNameByte)) {
                byte[] val = redisConnector.get(userNameByte);
                Map<String, Map<String, byte[]>> map = (Map<String, Map<String, byte[]>>) deserialize(val);
                if(map.containsKey(fileName)){
                    Map<String, byte[]> map1 = map.get(fileName);
                    map1.put(seqID, content);
                }
                else{
                    Map<String, byte[]> map2 = new HashMap<>();
                    map2.put(seqID, content);
                    map.put(fileName, map2);
                }

                String res = redisConnector.set(userNameByte, serialize(map));
                if (res == null) {
                    logger.info("Error storing in Redis " + userName);
                    return null;
                }

            } else {
                HashMap<String, Map<String, byte[]>> newMap = new HashMap<>();
                Map<String, byte[]> innerMap = new HashMap<>();
                innerMap.put(seqID, content);
                newMap.put(fileName, innerMap);
                String res = redisConnector.set(userNameByte, serialize(newMap));
                if (res == null) {
                    logger.info("Error storing in Redis for first time " + userName);
                    return null;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fileName;
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, byte[]>> getFilesMap(@NotNull String userName) {
        byte[] userEmailByte = serialize(userName);
        if (redisConnector.exists(userEmailByte)) {
            byte[] userData = redisConnector.get(userEmailByte);
            logger.info("Deserializing user data map " + userData);
            Map<String, Map<String, byte[]>> map = (Map<String, Map<String, byte[]>>) deserialize(userData);
            return map;
        }
        logger.info("User email not present");
        return null;
    }

    /**
     *
     * @param
     * @param
     * @return
     */
    public Map<String, byte[]> get(@NotNull String userName, @NotNull String fileName) {
        Map<String, Map<String, byte[]>> tempMap = getFilesMap(userName);
        if(tempMap == null || tempMap.isEmpty() || !tempMap.containsKey(fileName)){
            return null;
        }
        Map<String, byte[]> res = tempMap.get(fileName);
        return res;
    }

    /**
     *
     * @param userName
     * @param fileName
     * @return
     */
    public boolean remove(@NotNull String userName, @NotNull String fileName){
        Map<String, Map<String, byte[]>> tempMap = getFilesMap(userName);
        if(tempMap == null || tempMap.isEmpty() || !tempMap.containsKey(fileName)){
            return false;
        }
        Iterator<Map.Entry<String, Map<String, byte[]>>> itr = tempMap.entrySet().iterator();
        while(itr.hasNext()){
            Map.Entry<String, Map<String, byte[]>> entry = itr.next();
            if(fileName.equals(entry.getKey())){
                logger.info("Removing file ");
                itr.remove();
                break;
            }
        }
        logger.info("After deletion ----> " + tempMap);
        redisConnector.set(serialize(userName), serialize(tempMap));
        return true;
    }

    //TODO: Implement update file
    public boolean update(@NotNull String userName, @NotNull String fileName, String seqID, byte[] content) {
        Map<String, Map<String, byte[]>> tempMap = getFilesMap(userName);
        if(tempMap == null || tempMap.isEmpty() || !tempMap.containsKey(fileName)){
            return false;
        }
        Map<String, byte[]> newdata = new HashMap<>();
        newdata.put(seqID, content);
        redisConnector.set(serialize(userName), serialize(tempMap));
        return true;
    }

}
