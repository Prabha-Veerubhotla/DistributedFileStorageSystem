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
        System.out.println("put - store a file");
        System.out.println("get - retrieve a stored file");
        System.out.println("list - list all stored files");
        System.out.println("delete - delete a stored file");
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
                    rc.sendMessage(msg);
                } else if (choice.equalsIgnoreCase("help")) {
                    System.out.println("");
                    System.out.println("Commands");
                    System.out.println("-------------------------------");
                    System.out.println("help - show this menu");
                    System.out.println("post - send a message");
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
                    System.out.print("Enter file name to retrieve: ");
                    String msg = br.readLine();
                    byte[] filecontents = rc.getFile(msg);
                    //TODO: print the file name or the contents ?
                    System.out.println("Fetching file contents: ");
                } else if(choice.equalsIgnoreCase("list")) {
                    List<String> fileList = rc.listFiles();
                    for(String s: fileList) {
                        System.out.println(s);
                    }
                } else if(choice.equalsIgnoreCase("delete")) {
                    System.out.print("Enter file name to delete: ");
                    String msg = br.readLine();
                    boolean deleteStatus = rc.deleteFile(msg);
                    if(deleteStatus) {
                        System.out.println("File  "+msg+" successfully deleted");
                    }
                }
                else {
                    rc.sendMessage(choice);
                }
            } catch (Exception e) {
                forever = false;
                e.printStackTrace();
            }
        }

        System.out.println("\nGoodbye");
        rc.stopClientSession();
    }

    public static void main(String[] args) {
        Properties p = new Properties();
        p.setProperty("host", args[0]);
        p.setProperty("port", "2345");

        MessageClient ca = new MessageClient(p);
        ca.run();
    }
}
