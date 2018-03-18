package distributed;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Master
{
    private ServerSocket server;
    private WorkerManager workerManager;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        workerManager = new WorkerManager("resources/workers.config");
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
        workerManager.start();
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


}
