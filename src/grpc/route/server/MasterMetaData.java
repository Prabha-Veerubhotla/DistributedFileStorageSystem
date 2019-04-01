package grpc.route.server;

import com.google.rpc.Help;
import main.db.RedisHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

@SuppressWarnings("unchecked")
public class MasterMetaData {
    Logger logger = Logger.getLogger(MasterMetaData.class.getName());
    //{userName: {fileName : [ori_IP, rep_IP]}}

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
    public MasterMetaData() {
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
     * Check if file exists before PUT and UPDATE
     * @return
     */
    public boolean checkIfFileExists(String userName, String fileName){
        byte[] userNameByte = serialize(userName);
        try{
            if(redisConnector.exists(userNameByte)){
                byte[] val = redisConnector.get(userNameByte);
                Map<String, List<String>> userFilesMap = (Map<String, List<String>>)deserialize(val);
                if(userFilesMap.containsKey(fileName)){
                    return true;
                }
            }
            else {
                logger.info("User does not exists!");
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return false;
    }

    public boolean putMetaData(String userName, String fileName, String IP) {
        byte[] userNameByte = serialize(userName);
        try {
            if (redisConnector.exists(userNameByte)) {
                byte[] val = redisConnector.get(userNameByte);
                Map<String, List<String>> userFilesMap = (Map<String, List<String>>)deserialize(val);
                if (userFilesMap.containsKey(fileName)) {
                    List<String> IPList = userFilesMap.get(fileName);
                    IPList.add(IP);
                } else {
                    List<String> t = new ArrayList();
                    t.add(IP);
                    userFilesMap.put(fileName, t);
                }
                logger.info("userMap ----> " + userFilesMap);
                String res = redisConnector.set(userNameByte, serialize(userFilesMap));
                if(res == null){
                    logger.info("Error storing in Redis for first time " + userName);
                    return false;
                }
            } else {
                Map<String, List<String>> innerMap = new HashMap<>();
                List<String> IPList = new ArrayList<>();
                IPList.add(IP);
                innerMap.put(fileName, IPList);
                logger.info("newMap ----> " + innerMap);
                String res = redisConnector.set(userNameByte, serialize(innerMap));
                if (res == null) {
                    logger.info("Error storing in Redis for first time " + userName);
                    return false;
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        logger.info("Success " + userName);
        return true;
    }

    @SuppressWarnings("unchecked")
    public List<String> getMetaData(String userName, String fileName) {
        byte[] userNameByte = serialize(userName);
        try{
            if(redisConnector.exists(userNameByte)){
                byte[] val = redisConnector.get(userNameByte);
                Map<String, List<String>> userFilesMap = (Map<String, List<String>>)deserialize(val);
                if(userFilesMap.containsKey(fileName)){
                    return userFilesMap.get(fileName);
                }
                else {
                    logger.info("File not present!");
                    return null;
                }
            }
            else {
                logger.info("User not present");
                return null;
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Get all files for a user from MetaData
     *
     * @param userName
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set<String> getAllFiles(String userName) {
        byte[] userNameByte = serialize(userName);
        try {
            if (redisConnector.exists(userNameByte)) {
                byte[] val = redisConnector.get(userNameByte);
                Map<String, List<String>> allFiles = (Map<String, List<String>>) deserialize(val);
                logger.info("All user Files: " + allFiles);
                return allFiles.keySet();
            } else {
                logger.info("User not present");
                return null;
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public void deleteFileFormMetaData(String userName, String fileName) {
        byte[] userNameByte = serialize(userName);
        try {
            if (redisConnector.exists(userNameByte)) {
                byte[] val = redisConnector.get(userNameByte);
                Map<String, List<String>> allFiles = (Map<String, List<String>>) deserialize(val);
                if(allFiles.containsKey(fileName)){
                    allFiles.remove(fileName);
                    redisConnector.set(userNameByte, serialize(allFiles));
                    logger.info("File Removed Successfully!");
                }
                else {
                    logger.info("File not present!");
                }

            } else {
                logger.info("User not present");
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public List<String> updateMetaData(String userName, String fileName){
        return getMetaData(userName, fileName);
    }

//    public static void main(String[] args){
//        MasterMetaData md = new MasterMetaData();
//        md.putMetaData("Nrupa", "pic.jpg","10.250.10.320");
//        md.putMetaData("Nrupa", "pic1.jpg","10.250.10.321");
//        md.putMetaData("Nrupa", "pic1.jpg", "10.250.10.320");


//        Set<String> ans = md.getAllFiles("Nrupa");
//        System.out.println("ans: " + ans);

//        List<String> ans = md.getMetaData("Nrupa", "pic1.jpg");
//        System.out.println("ans: " + ans);
//        List<String> ans = md.updateMetaData("Nrupa", "pic.jpg");
//        System.out.println("Ans: " + ans);
//        boolean flag = md.checkIfFileExists("Nrupa", "pic2.jpg");
//        System.out.println("Flag: " + flag);
//        md.deleteFileFormMetaData("Nrupa", "pic.jpg");
//    }
}
