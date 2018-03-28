package distributed;

import javax.swing.*;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Worker
{

    private ObjectOutputStream out;
    private ObjectInputStream in;
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
            providerSocket = new ServerSocket(6666, 10);
            // Accept the connection
            connection = providerSocket.accept();
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
            System.out.println("New connection..");

            freeMemory = Runtime.getRuntime().freeMemory();
            totalMemory = Runtime.getRuntime().totalMemory();
            //System.out.println("freeMem= " + freeMemory / 1000000 + " totalMem= " + totalMemory / 1000000);
            numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            //System.out.println(numberOfProcessors);

            RamCpuStats = String.valueOf(freeMemory) + ";" + String.valueOf(numberOfProcessors);
            // I added the while loop to test the master/worker communication.
            while (true)
            {
                try
                {
                    String msg = (String) in.readObject();
                    if (msg.equals("status"))
                    {
                        out.writeObject(RamCpuStats);
                        out.flush();
                    } else if (msg.equals("train"))
                    {
                        System.out.println("Starting the training");
                        List<Integer> data = (ArrayList<Integer>) in.readObject();
                        data.sort((Integer l, Integer r) ->
                        {
                            return l - r;
                        });
                        out.writeObject(data);
                        out.flush();
                    }
                } catch (ClassNotFoundException cnfe)
                {
                    //pass
                }
            }


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
