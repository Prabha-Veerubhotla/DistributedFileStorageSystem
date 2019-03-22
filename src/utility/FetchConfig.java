package utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FetchConfig {
    protected static Logger logger = LoggerFactory.getLogger("Fetch-Config");
    private static List<String> msgTypes = Arrays.asList("join", "get", "put", "list", "delete", "request-ip", "save-client-ip", "slave-ip");


    public static Properties getConfiguration(final File path) throws IOException {
        if (!path.exists())
            throw new IOException("missing file");

        Properties rtn = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            rtn.load(fis);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    logger.info("Unable to read from config file: " + path + " due to exception: " + e);
                }
            }
        }
        return rtn;
    }

    public static List<String> getMsgTypes() {
        return msgTypes;
    }

}
