package distributed;

import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;

public class Worker
{

    private ObjectOutputStream out;
    private long freeMemory;
    private long totalMemory;
    private long maxMemory;
    private int numberOfProcessors;
    private String RamCpuStats;

    public static void main(String args[])
    {
        new Worker().openServer();
    }

    private ServerSocket providerSocket;
    private Socket connection = null;

    public void openServer()
    {
        try
        {
            providerSocket = new ServerSocket(6667, 10);


            // Accept the connection
            connection = providerSocket.accept();

            System.out.println("New connection..");

            freeMemory = Runtime.getRuntime().freeMemory();
            totalMemory = Runtime.getRuntime().totalMemory();
            System.out.println("freeMem= " + freeMemory / 1000000 + " totalMem= " + totalMemory / 1000000);
            numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            System.out.println(numberOfProcessors);

            RamCpuStats = String.valueOf(freeMemory) + "." + String.valueOf(numberOfProcessors);


            out = new ObjectOutputStream(connection.getOutputStream());
            out.writeObject(RamCpuStats);
            out.flush();


        } catch (IOException ioException)
        {
            ioException.printStackTrace();
        } finally
        {
            try
            {
                out.close();
                providerSocket.close();
            } catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }
    }
}
