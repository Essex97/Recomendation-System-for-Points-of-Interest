package distributed;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.*;
import java.net.*;

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


            System.out.println("hello");
            connection = providerSocket.accept();

            System.out.println("New connection..");

            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject("teststring");
            out.close();


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
