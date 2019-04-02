package main.db;
import com.sun.istack.NotNull;
import main.entities.FileEntity;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;


public class RedisHandler {
    Logger logger = Logger.getLogger(RedisHandler.class.getName());
    private Jedis redisConnector;
    public static JedisPool redisPool;
    public static final int MAX_POOL_SIZE = 100;
    public static final String HOST_NAME = "localhost";
    private static boolean FIRST_UPDATE_CALL = true;

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
        logger.info("Inside PUT redis handler");
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
        logger.info("Inside GET redis handler");
        Map<String, Map<String, byte[]>> tempMap = getFilesMap(userName);
        if(tempMap == null || tempMap.isEmpty() || !tempMap.containsKey(fileName)){
            return null;
        }
        Map<String, byte[]> res = tempMap.get(fileName);
        logger.info("Sending file to client!");
        return res;
    }

    /**
     *
     * @param userName
     * @param fileName
     * @return
     */
    public boolean remove(@NotNull String userName, @NotNull String fileName){
        logger.info("Inside REMOVE redis handler");
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

    public void update(@NotNull String userName, @NotNull String fileName, String seqID, byte[] content) {
        logger.info("Inside UPDATE redis handler");
        if(FIRST_UPDATE_CALL){
            logger.info("First update!");
            remove(userName, fileName);
            FIRST_UPDATE_CALL = false;
            Map<String, Map<String, byte[]>> res = getFilesMap(userName);
            if(res.containsKey(fileName)){
                logger.info("File Present");
            }
            else{
                logger.info("File not present");
            }
        }
        put(userName, fileName, seqID, content);
    }

    //TODO: Move to client - wrote here for testing purposes
    @SuppressWarnings("unchecked")
    public static byte[] combineBytes(Map<String, byte[]> res) {
        List<String> sortedKeys = new ArrayList(res.keySet());
        sortedKeys.sort(Comparator.comparingInt(Integer::parseInt));
        List<byte[]> allbytes = new ArrayList<>();
        for (String sortedKey : sortedKeys) {
            allbytes.add(res.get(sortedKey));
        }
        System.out.println("Total Size: " + allbytes.size());
        List<Byte> allData = new ArrayList<>();
        for (byte[] allbyte : allbytes) {
            for (byte anAllbyte : allbyte) {
                allData.add(anAllbyte);
            }
        }

        byte[] b = new byte[allData.size()];
        for (int i = 0; i < allData.size(); i++) {
            b[i] = allData.get(i);
        }
        System.out.println("Total BSize: " + b.length);
        return b;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        RedisHandler rh = new RedisHandler();
        String filePath = "/Users/nrupa/Desktop/cat.png";
        String filePath1 = "/Users/nrupa/Desktop/test.txt";
        String fileName = "example.txt";
        String userName = "prabha";
        long seq = 0l;
        try {
//            FileInputStream fis = new FileInputStream(filePath1);
//            int i = 0;
//            do {
//                byte[] buf = new byte[1024];
//                i = fis.read(buf);
//                if (i != -1) {
//                    rh.update(userName, fileName, Long.toString(seq), buf);
//                }
//                seq++;
//            } while (i != -1);
//            byte[] payload = null;
            Map<String, byte[]> res = rh.get(userName, fileName);
            byte[] temp = combineBytes(res);
            BufferedOutputStream bw = null;
            bw = new BufferedOutputStream(new FileOutputStream("tempRedis.txt"));
            bw.write(temp);
            bw.flush();
            bw.close();
//            logger.info("Putting into DB");
//            mh.put(userName, new FileEntity(filePath, res));
//            FileEntity mongoDBres = mh.get(userName, filePath);
//            Map<String, byte[]> r = (Map<String, byte[]>) mongoDBres.getFileContents();
//            temp = combineBytes(res);
//            bw = null;
//            bw = new BufferedOutputStream(new FileOutputStream("tempMongo.jpg"));
//            bw.write(temp);
//            bw.flush();
//            bw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
