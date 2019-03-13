package main.db;

import com.sun.istack.internal.NotNull;
import main.entities.FileEntity;
import java.util.logging.Logger;

public interface DbHandler {
    String put(@NotNull String userEmail, @NotNull FileEntity file);

    FileEntity get(@NotNull String email, @NotNull String fileName);

    void remove(@NotNull String email, @NotNull String fileName);

    FileEntity update(@NotNull String email, @NotNull FileEntity newFile);

    Logger logger = Logger.getLogger(DbHandler.class.getName());

    void initDatabaseHandler() throws Exception;
}
