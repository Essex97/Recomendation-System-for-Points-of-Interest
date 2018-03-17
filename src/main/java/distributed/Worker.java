package distributed;

import java.io.ObjectInputStream;
import java.net.Socket;
import java.io.*;
import java.net.*;

// Test comment
public class Worker {
    public static void main(String args[]) {
        new Worker().openServer();
    }

    ServerSocket providerSocket;
    Socket connection = null;
    void openServer() {
        try {
            providerSocket = new ServerSocket(6666,10);


            System.out.println("hello");
            connection = providerSocket.accept();

            System.out.println("New connection..");


        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
