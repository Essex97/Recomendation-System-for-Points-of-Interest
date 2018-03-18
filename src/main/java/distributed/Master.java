package distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Master
{
    private HashMap<String, WorkerConnection> workers;
    private ServerSocket server;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        workers = new HashMap<>();
    }

    /**
     * This method initializes the workers map with a pair
     * of <worker name, WorkerConnection> for each worker that is
     * specified to the config file.
     *
     * @param path This is the path to the workers.config file.
     */
    private void wakeUpWorkers(String path)
    {
        ArrayList<String> lines = new ArrayList<>();
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
            } catch (IOException ioe)
            {
                ioe.printStackTrace();
            } catch (NullPointerException npe)
            {
                npe.printStackTrace();
            }
        }
        int i = 0;
        try
        {
            for (; i < lines.size(); i++)
            {
                String[] tokens = lines.get(i).split(" ");
                int port = Integer.parseInt(tokens[2]);
                Socket connection = new Socket(tokens[1], port);
                workers.put(tokens[0], new WorkerConnection(connection, tokens[0]));
            }
        } catch (IOException ioe)
        {
            String[] tokens = lines.get(i).split(" ");
            System.out.println("Failed To connect to worker " + tokens[0] + " with IP: " + tokens[1]);
            ioe.printStackTrace();
        } catch (NumberFormatException nfe)
        {
            nfe.printStackTrace();
        }
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

    /**
     * This method gets the CPU, RAM status of each worker.
     */
    public void getWorkerStatus()
    {
        for (WorkerConnection manager : workers.values())
        {
            String status = (String) manager.readData();
            System.out.println("Worker stats: " + status);
            manager.close();
        }
    }
}
