package distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Master
{
    private ServerSocket server;
    private HashMap<String, WorkerConnection> workers;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        workers = new HashMap<String, WorkerConnection>();
    }

    public static void main(String[] args)
    {
        new Master().startMaster();
    }

    /**
     * This method starts the master.
     */
    public void startMaster()
    {
        wakeUpWorkers("resources/workers.config");
        getWorkerStatus();
        listenForConnections();
    }

    /**
     * This method initializes the workers map with a pair
     * of <worker name, WorkerConnection> for each worker that is
     * specified to the config file.
     *
     * @param path This is the path to the workers.config file.
     */
    public void wakeUpWorkers(String path)
    {
        ArrayList<String> lines = new ArrayList<String>();
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new FileReader(new File(path)));
            String line;
            while ((line = br.readLine()) != null)
                lines.add(line);
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        } catch (NullPointerException npe)
        {
            npe.printStackTrace();
        } finally
        {
            try
            {
                br.close();
            } catch (NullPointerException npe)
            {
                npe.printStackTrace();
            } catch (IOException ioe)
            {
                ioe.printStackTrace();
            }
        }
        int i = 0;
        for (; i < lines.size(); i++)
        {
            String[] tokens = lines.get(i).split(" ");
            int port = 0;
            try
            {
                port = Integer.parseInt(tokens[2]);
                Socket connection = new Socket(tokens[1], port);
                workers.put(tokens[0], new WorkerConnection(connection, tokens[0]));
                System.out.println("Connected to worker " + tokens[0] + " with IP: " + tokens[1]);
            } catch (IOException ioe)
            {
                System.out.println("Failed To connect to worker " + tokens[0] + " with IP: " + tokens[1] + " on port " + port);
                //ioe.printStackTrace();
            } catch (NumberFormatException nfe)
            {
                nfe.printStackTrace();
            }
        }
    }

    /**
     * This method gets the CPU, RAM status of each worker.
     */
    public void getWorkerStatus()
    {
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (WorkerConnection connection : workers.values())
        {
            Thread job = new Thread(() ->
            {
                connection.sendData("status");
                String msg = (String) connection.readData();
                System.out.println(connection.getName() + ": " + msg);
                String[] tokens = msg.split(";");
                connection.setCpuCores(Integer.parseInt(tokens[1]));
                connection.setMemory(Integer.parseInt(tokens[0]));
            });
            threads.add(job);
            job.start();
        }

        for (Thread job : threads)
        {
            try
            {
                job.join();
            } catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
        }

        /*for (WorkerConnection connection : workers.values())
        {
            System.out.println(connection.getName() + " " + connection.getCpuCores() + " cores\n" + connection.getMemory() + " free memory.");
        }*/
    }

    /**
     * This method is responsible for managing all client connections
     * by spawning a thread for each one.
     */
    public void listenForConnections()
    {
        try
        {
            server = new ServerSocket(7777, 5);
            System.out.println("Listening for client connections...");
            // Run indefinitely.
            while (true)
            {
                Socket client = server.accept();
                System.out.println("Client connected.");
                ClientConnection manager = new ClientConnection(client);
                manager.start();
            }
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        } finally
        {
            try
            {
                server.close();
            } catch (IOException ioe)
            {
                ioe.printStackTrace();
            }
        }
    }
}
