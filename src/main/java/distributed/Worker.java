package distributed;
// To set the memory used by the JVM in Intellij Alt+Shift+F10 -> Edit Configuration -> VM options: -Xmx2000m

import org.apache.commons.math3.linear.OpenMapRealMatrix;

import javax.swing.*;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Worker
{
    // Used to print max memory used by the JVM
    static String mb(long s)
    {
        return String.format("%d (%.2f M)", s, (double) s / (1024 * 1024));
    }

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int numberOfProcessors;
    private String RamCpuStats;

    public static void main(String args[])
    {
        new Worker().startWorker();
    }

    private ServerSocket providerSocket;
    private Socket connection = null;

    public void startWorker()
    {
        try
        {
            providerSocket = new ServerSocket(6667, 10);
            // Accept the connection
            connection = providerSocket.accept();
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());

            System.out.println("New connection..");
            System.out.println("Runtime max memory: " + mb(Runtime.getRuntime().maxMemory()));

            numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            RamCpuStats = String.valueOf(Runtime.getRuntime().maxMemory()) + ";" + String.valueOf(numberOfProcessors);

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
                        OpenMapRealMatrix data = (OpenMapRealMatrix) in.readObject();
                        //out.writeObject(data);
                        //out.flush();
                        for (int i = 0; i < data.getRowDimension(); i++)
                        {
                            for (int j = 0; j < data.getColumnDimension(); j++)
                            {
                                data.setEntry(i, j, 7);
                            }
                        }
                        System.out.println("total rows " + data.getRowDimension());
                        out.writeObject(data);
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
