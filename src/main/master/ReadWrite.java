package main.master;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadWrite {
    public static final int TOTALPARTITIONS = 3;

    /**
     * Divides file into 3 partitions of byte array
     * @param fileName
     * @param filePath
     * @return List of 3 byte array
     */
    public List<byte[]> convertFileToByteArray(String fileName, String filePath){
        List<byte[]> ans = new ArrayList<>();
        try {
            RandomAccessFile f = new RandomAccessFile(filePath, "r");
            byte[] b = new byte[(int) f.length()];
            f.readFully(b);
            f.close();
            int chunk = (int) b.length / TOTALPARTITIONS;
            ans.add(Arrays.copyOfRange(b, 0, chunk));
            ans.add(Arrays.copyOfRange(b, chunk , 2*chunk));
            ans.add(Arrays.copyOfRange(b, 2*chunk, b.length));
            return ans;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Merges list of 3 byte arrays to give back file
     * @param fileName
     * @param allbytes
     */
    public void convertByteArrayToFile(String fileName, List<byte[]> allbytes){
        byte[] allData = new byte[allbytes.get(0).length + allbytes.get(1).length + allbytes.get(2).length];
        int k = 0;
        for(int i = 0; i < allbytes.size(); i++){
            for(int j = 0; j < allbytes.get(i).length; j++){
                allData[k] = allbytes.get(i)[j];
                k++;
            }
        }
        BufferedOutputStream bw = null;
        try {
            bw = new BufferedOutputStream(new FileOutputStream(fileName));
            bw.write(allData);
            bw.flush();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
