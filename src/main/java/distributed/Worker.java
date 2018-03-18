package distributed;

import java.io.ObjectInputStream;
import java.net.Socket;
import java.io.*;
import java.net.*;

// Testcomment
public class Worker {

    ObjectOutputStream out;

    public static void main(String args[]) {
        new Worker().openServer();
    }

    ServerSocket providerSocket;
    Socket connection = null;
    void openServer() {
        try {
            providerSocket = new ServerSocket(6666,10);


            // Accept the connection
            connection = providerSocket.accept();

            System.out.println("New connection..");
            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("testString");
            out.flush();


        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                out.close();
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
