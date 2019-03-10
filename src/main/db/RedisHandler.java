package main.db;

import com.sun.istack.internal.NotNull;
import main.entities.FileEntity;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class RedisHandler implements DbHandler {
    private Jedis redisConnector;
    public static JedisPool redisPool;
    public static final int MAX_POOL_SIZE = 10;
    public static final String HOST_NAME = "localhost";

    public Jedis getRedisConnector() {
        return redisConnector;
    }

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
     * Constructor to initialize Jedis instance
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
        logger.info("Serializing data");
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
            try {
                out = oin.readObject();
            } catch (ClassNotFoundException ex) {
            }
        } catch (IOException ex) {
        }
        return out;
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    @Override
    public String put(@NotNull String userEmail, @NotNull FileEntity file) {
        byte[] userEmailByte = serialize(userEmail);
        try {
            if (redisConnector.exists(userEmailByte)) {
                byte[] val = redisConnector.get(userEmailByte);
                Map<String, FileEntity> map = (Map<String, FileEntity>) deserialize(val);
                map.put(file.toString(), file);
                String res = redisConnector.set(userEmailByte, serialize(map));
                if (res == null) {
                    logger.info("Error storing in Redis " + userEmail);
                    return null;
                }
                logger.info("Success " + userEmail);
            } else {
                Map<String, FileEntity> newMap = new HashMap<String, FileEntity>();
                newMap.put(file.toString(), file);
                String res = redisConnector.set(userEmailByte, serialize(newMap));
                if (res == null) {
                    logger.info("Error storing in Redis for first time " + userEmail);
                    return null;
                }
                logger.info("Success " + userEmail);
            }
        } catch (Exception ex) {
        }
        return userEmail;
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private Map<String, FileEntity> getFilesMap(@NotNull String email) {
        byte[] userEmailByte = serialize(email);
        if (redisConnector.exists(userEmailByte)) {
            byte[] userData = redisConnector.get(userEmailByte);
            logger.info("Deserializing user data map");
            Map<String, FileEntity> map = (Map<String, FileEntity>) deserialize(userData);
            return map;
        } else {
            logger.warning("User email not present");
            return null;
        }
    }

    /**
     * @param email
     */
//    @Override
    public Map<String, FileEntity> get(@NotNull String email) {
        return getFilesMap(email);

    }

    /**
     *
     * @param email
     * @param fileName
     * @return
     */
    @Override
    public FileEntity get(@NotNull String email, @NotNull String fileName) {
        Map<String, FileEntity> tempMap = getFilesMap(email);
        logger.info("Deserialized map " + tempMap.get(fileName));
        return tempMap.get(fileName);
    }

    /**
     *
     * @param email
     * @param fileName
     * @return
     */
    @Override
    public void remove(@NotNull String email, @NotNull String fileName){
        Map<String, FileEntity> tempMap = getFilesMap(email);
        FileEntity removedFile = tempMap.remove(fileName);
        redisConnector.set(serialize(email), serialize(tempMap));
        if(removedFile != null){
            logger.info("Removed file " + fileName);
        }
        else{
            logger.warning("File does not exist!");
        }
    }

    @Override
    public FileEntity update(String email, FileEntity newFile) {
        return null;
    }

    @Override
    public void initDatabaseHandler() throws Exception {

    }
}
