package message;

import grpc.route.client.RouteClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

/**
 * console interface to the socket example
 *
 * @author gash
 *
 */
public class MessageClient {
    private Properties setup;

    public MessageClient(Properties setup) {
        this.setup = setup;
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
        rc.join(name);

        System.out.println("\nWelcome " + name + "\n");
        System.out.println("Commands");
        System.out.println("-----------------------------------------------");
        System.out.println("help - show this menu");
        System.out.println("post - send a message to the group (default)");
        System.out.println("whoami - list my settings");
        //System.out.println("put - store a file");
        System.out.println("get - retrieve a stored message");
        //System.out.println("get-file - retrieve a stored file");
        System.out.println("list - list all stored messages");
        System.out.println("delete - delete a stored message");
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
                } else if (choice.equalsIgnoreCase("post")) {
                    System.out.print("Enter message: ");
                    String msg = br.readLine();
                    boolean msgstatus = rc.putString(msg);
                    if(msgstatus) {
                        System.out.println(msg+ " saved successfully");
                    } else {
                        System.out.println(msg+ " not saved successfully");
                    }
                } else if (choice.equalsIgnoreCase("help")) {
                    System.out.println("");
                    System.out.println("Commands");
                    System.out.println("-------------------------------");
                    System.out.println("help - show this menu");
                    System.out.println("post - send a message");
                    System.out.println("whoami - list my settings");
                    //System.out.println("put - store a file");
                    System.out.println("get - retrieve a stored message");
                    System.out.println("list - list all stored messages");
                    System.out.println("delete - delete a stored message");
                    System.out.println("exit - end session");
                    System.out.println("");
                } else if(choice.equalsIgnoreCase("put")) {
                    System.out.print("Enter the relative path of file with the file name: ");
                    System.out.println("Example Usage:  /Users/Desktop/project1.txt");
                    System.out.print("Enter file path: ");
                    String msg = br.readLine();
                    boolean putStatus = rc.putFile(msg);
                    if(putStatus) {
                        System.out.println("Successfully stored file: "+msg);
                    }
                } else if(choice.equalsIgnoreCase("get")) {
                    System.out.print("Enter message to retrieve: ");
                    String msg = br.readLine();
                    //byte[] filecontents = rc.getFile(msg);
                    //TODO: print the file name or the contents ?
                    String received = rc.getString(msg);
                    if(received.equalsIgnoreCase("")) {
                        System.out.println("unable to retrieve message: "+msg);
                        System.out.println("message: "+msg+" not saved or deleted");
                    } else {
                        System.out.println("message retrived..");
                        System.out.println(received);
                    }
                   // System.out.println("Fetching file contents: ");
                } else if(choice.equalsIgnoreCase("get-file")) {
                    System.out.print("Enter file name to retrieve: ");
                    String msg = br.readLine();
                    //byte[] filecontents = rc.getFile(msg);
                    //TODO: print the file name or the contents ?
                    byte[] content = rc.getFile(msg);
                    System.out.println("Fetching file contents: ");
                    System.out.println(new String(content));
                } else if(choice.equalsIgnoreCase("list")) {
                    List<String> msgList = rc.listMessages();
                    if(msgList.size() != 0) {
                        for (String s : msgList) {
                            System.out.println(s);
                        }
                    } else {
                        System.out.println("no messages saved for this user: "+name);
                    }
                } else if(choice.equalsIgnoreCase("delete")) {
                    System.out.print("Enter message name to delete: ");
                    String msg = br.readLine();
                    boolean deleteStatus = rc.deleteMessage(msg);
                    if(deleteStatus) {
                        System.out.println("Message  "+msg+" successfully deleted");
                    }
                }
                else {
                    rc.putString(choice);
                }
            } catch (Exception e) {
                forever = false;
                e.printStackTrace();
            }
        }

        System.out.println("\nGoodbye");
        rc.stopClientSession();
        System.exit(0);
    }

    public static void main(String[] args) {
        Properties p = new Properties();
        p.setProperty("host", args[0]);
        p.setProperty("port", "2345");

        MessageClient ca = new MessageClient(p);
        ca.run();

    }
}
