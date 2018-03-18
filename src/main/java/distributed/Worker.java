package distributed;

import java.io.ObjectInputStream;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;

// Testcommentyol I have to study Computer Networks
// Testcommentyo
// Testcomment2 testing merge
// Merge is done :)
public class Worker {

    ObjectOutputStream out;
    long freeMemory;
    long totalMemory;
    long maxMemory;
    int numberOfProcessors;
    String RamCpuStats;

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

            freeMemory = Runtime.getRuntime().freeMemory();
            totalMemory = Runtime.getRuntime().totalMemory();
            System.out.println("freeMem= "+freeMemory/1000000+" totalMem= "+totalMemory/1000000);
            numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            System.out.println(numberOfProcessors);

            RamCpuStats = String.valueOf(freeMemory)+"."+String.valueOf(numberOfProcessors);




            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(RamCpuStats);
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
