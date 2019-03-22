package message;

import grpc.route.client.RouteClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

    public static void choiceHandler(String choice, RouteClient rc) throws IOException {

        try {

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            if (choice.equalsIgnoreCase("post")) {
                System.out.print("Enter message or file path: ");
                String msg = br.readLine();
                boolean msgstatus = rc.put(msg);
                if (msgstatus) {
                    System.out.println(msg + " saved successfully");
                } else {
                    System.out.println(msg + " not saved successfully");
                }

            } else if (choice.equalsIgnoreCase("get")) {
                System.out.print("Enter message or file name to retrieve: ");
                String msg = br.readLine();
                byte[] received = rc.get(msg); // pending: change this method call
                System.out.println("Retrieved file or message in byte array format: " + received);
                System.out.println("Retrieved file or message in string format:" + received.toString());

            } else if (choice.equalsIgnoreCase("list")) {
                List<String> msgList = rc.list();
                if (msgList.size() != 0) {
                    for (String s : msgList) {
                        System.out.println(s);
                    }
                } else {
                    System.out.println("no messages or files saved from this user: " + clientname);
                }
            } else if (choice.equalsIgnoreCase("delete")) {
                System.out.print("Enter message or file name to delete: ");
                String msg = br.readLine();
                boolean deleteStatus = rc.delete(msg);
                if (deleteStatus) {
                    System.out.println("Message  " + msg + " successfully deleted");
                } else {
                    System.out.println("Delete operation failed for: " + msg);
                }
            } else {

                // if the choice is not one of the above options { get, post, list, delete }
                // the default is post
                rc.put(choice);
            }
        } catch (IOException ie) {
            System.out.println("Exception: " + ie + " while handling the client choice: " + choice);
            throw new IOException();
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
        rc.startClientSession();
        rc.setName(name);
        clientname = name;
        if (!rc.join()) {
            rc.stopClientSession();
            System.exit(0);
        }

        System.out.println("\nWelcome " + name + "\n");
        System.out.println("Commands");
        System.out.println("-----------------------------------------------");
        System.out.println("help - show this menu");
        System.out.println("post - send a file or message(default)");
        System.out.println("whoami - list my settings");
        System.out.println("get - retrieve a stored message or file");
        System.out.println("list - list all stored messages or files");
        System.out.println("delete - delete a stored message or file");
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
                    System.out.println("post - save a message or file(default)");
                    System.out.println("whoami - list my settings");
                    System.out.println("get - retrieve a stored message or file");
                    System.out.println("list - list all stored messages or files");
                    System.out.println("delete - delete a stored message or file");
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
            System.out.println("\nGoodbye");
            rc.stopClientSession();
            System.exit(0);
        }
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

 /*else if(choice.equalsIgnoreCase("put-file")) {
                    System.out.print("Enter the relative path of file with the file name: ");
                    System.out.println("Example Usage:  /Users/Desktop/project1.txt");
                    System.out.print("Enter file path: ");
                    String msg = br.readLine();
                    boolean putStatus = rc.putFile(msg);
                    if(putStatus) {
                        System.out.println("Successfully stored file: "+msg);
                    }
                }*/

 /*else if(choice.equalsIgnoreCase("get-file")) {
                    System.out.print("Enter file name to retrieve: ");
                    String msg = br.readLine();
                    //byte[] filecontents = rc.getFile(msg);
                    //TODO: print the file name or the contents ?
                    byte[] content = rc.getFile(msg);
                    System.out.println("Fetching file contents: ");
                    System.out.println(new String(content));
                }*/