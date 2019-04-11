package main.entities;

import com.sun.istack.NotNull;

public class FileEntity implements java.io.Serializable{
    private String fileName;
    private Object fileContents;


    public String getFileName() {
        return fileName;
    }

    public Object getFileContents() {
        return fileContents;
    }

    public FileEntity(@NotNull String name, @NotNull Object cont){
        fileName = name;
        fileContents = cont;
    }

    @Override
    public String toString(){
        return fileName;
    }
}
