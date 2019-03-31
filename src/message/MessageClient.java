package message;

import grpc.route.client.RouteClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * console interface to the socket example
 *
 * @author gash
 */
public class MessageClient {
    private Properties setup;
    private static String clientname;
    //print on console only in this file

    public MessageClient(Properties setup) {
        this.setup = setup;
    }

    public static void choiceHandler(String choice, RouteClient rc) {

        try {

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            if (choice.equalsIgnoreCase("put")) {
                System.out.print("Enter file path: ");
                String msg = br.readLine();
                System.out.println(rc.streamFileToServer(msg));

            } else if (choice.equalsIgnoreCase("get")) {
                System.out.print("Enter file name to retrieve: ");
                String msg = br.readLine();
                File f = new File("sample.txt"); //= rc.download(msg);
                System.out.println("Retrieved file: " + f);
            } else if (choice.equalsIgnoreCase("list")) {
                String list = rc.listFilesInServer(clientname);
                if( list!= null) {
                    System.out.println(list);
                } else {
                    System.out.println("No files saved from this user: " + clientname);
                }
            } else if (choice.equalsIgnoreCase("delete")) {
                System.out.print("Enter file name to delete: ");
                String msg = br.readLine();
                boolean deleteStatus = rc.deleteFileFromServer(msg);
                if (deleteStatus) {
                    System.out.println("File  " + msg + " successfully deleted");
                } else {
                    System.out.println("Delete operation failed for: " + msg);
                }
            } else if (choice.equalsIgnoreCase("update")) {
                System.out.print("Enter file name to update: ");
                String msg = br.readLine();
                String updateStatus = rc.updateFileInServer(msg);
                System.out.println(updateStatus);
            } else if(choice.equalsIgnoreCase("search")) {
                System.out.print("Enter file name to update: ");
                String msg = br.readLine();
                boolean searchResult = rc.searchFileInServer(msg);
                if(searchResult) {
                    System.out.println("File "+ msg+ " is present. Enter put and file name to save it");
                } else {
                    System.out.println("File: "+msg+" is not present in the server");
                }

            }
            else {

                // if the choice is not one of the above options { get, put, list, delete }
                // the default is put
                rc.streamFileToServer(choice);
            }
        } catch (IOException ie) {
            System.out.println("Exception: " + ie + " while handling the client choice: " + choice);
        }

    }

    //Read inputs from the user
    public void run() {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String name = null;
        do {
            try {
                if (name == null) {
                    System.out.print("Enter your name in order to join: ");
                    System.out.flush();
                    name = br.readLine();
                }
                System.out.println("");
            } catch (Exception e2) {
            }

            if (name != null)
                break;
        } while (true);

        RouteClient rc = new RouteClient(setup);
        rc.setName(name);
        clientname = name;
        rc.startClientSession();


        System.out.println("\nWelcome " + name + "\n");
        System.out.println("Commands");
        System.out.println("-----------------------------------------------");
        System.out.println("help - show this menu");
        System.out.println("whoami - list my settings");
        System.out.println("put - send a file (default)");
        System.out.println("get - retrieve a stored  file");
        System.out.println("update - update a stored file");
        System.out.println("list - list all stored  files");
        System.out.println("delete - delete a stored file");
        System.out.println("search - know if a file is present");
        System.out.println("exit - end session");
        System.out.println("");

        boolean forever = true;
        while (forever) {
            try {

                String choice = br.readLine();

                System.out.println("");
                if (choice == null) {
                    continue;
                } else if (choice.equalsIgnoreCase("whoami")) {
                    System.out.println("You are " + rc.getName());
                } else if (choice.equalsIgnoreCase("exit")) {
                    System.out.println("EXIT CMD!");
                    rc.stopClientSession();
                    forever = false;
                } else if (choice.equalsIgnoreCase("help")) {
                    System.out.println("");
                    System.out.println("Commands");
                    System.out.println("-------------------------------");
                    System.out.println("help - show this menu");
                    System.out.println("whoami - list my settings");
                    System.out.println("put - send a file (default)");
                    System.out.println("get - retrieve a stored  file");
                    System.out.println("update - update a stored file");
                    System.out.println("list - list all stored  files");
                    System.out.println("delete - delete a stored file");
                    System.out.println("search - to know if a file is present");
                    System.out.println("exit - end session");
                    System.out.println("");
                } else {
                    // if the choice is other than the basic stuff
                    // call choicehandler
                    choiceHandler(choice, rc);
                }
            } catch (Exception e) {
                forever = false;
            }
        }
        System.out.println("\nGoodbye");
        rc.stopClientSession();
        System.exit(0);

    }

    public static void main(String[] args) {
        // client config properties
        Properties p = new Properties();
        p.setProperty("host", args[0]);
        p.setProperty("port", "2345");

        MessageClient ca = new MessageClient(p);
        ca.run();

    }
}