package distributed;

import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Master
{
    private ReentrantLock lock; // A lock for the array of data.
    //private Condition trainCond, dontTrainCond;
    private ServerSocket server;
    private WorkerManager workerManager;
    //private Timer trainingScheduler;
    //private Boolean train;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        //train = new Boolean(false);
        //trainingScheduler = new Timer(true);
        lock = new ReentrantLock();
        //trainCond = lock.newCondition();
        //dontTrainCond = lock.newCondition();
        workerManager = new WorkerManager("resources/workers.config", lock);
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
        /*trainingScheduler.schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                lock.lock();
                try
                {
                    while (!train)
                        dontTrainCond.wait();
                    System.out.println("Timer is activated!");
                    train = true;
                    trainCond.signal();
                }catch (InterruptedException ie)
                {
                    ie.printStackTrace();
                }finally
                {
                    lock.unlock();
                }
            }
        }, 10*1000, 1*60*1000);*/
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
