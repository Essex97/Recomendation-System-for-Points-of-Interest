package distributed;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


// Temporary solution.
class Job extends Thread
{
    private final int mode;
    private WorkerConnection connection;
    private static final ReentrantLock lock = new ReentrantLock();

    public Job(WorkerConnection connection, int mode)
    {
        this.connection = connection;
        this.mode = mode;
    }

    @Override
    public void run()
    {
        switch (mode)
        {
            case 0:
                askStatus();
                break;
            case 1:
                train();
                break;
            default:
                break;
        }
    }

    private void train()
    {
        // Train the worker.
    }

    private void askStatus()
    {
        String status = (String) connection.readData();
        lock.lock();
        System.out.println("Worker stats: " + status);
        lock.unlock();
        connection.close();
    }
}

public class WorkerManager extends Thread
{
    private String configFile;
    private final ReentrantLock arrayLock;
    //private Condition trainCond, dontTrainCond;
    //private Boolean train;
    private HashMap<String, WorkerConnection> workers;

    public WorkerManager(String configPath, ReentrantLock arrayLock)
    {
        //this.train = train;
        this.arrayLock = arrayLock;
        //this.trainCond = trainCond;
        //this.dontTrainCond = dontTrainCond;
        configFile = configPath;
        workers = new HashMap<>();
    }

    @Override
    public void run()
    {


        while (true)
        {
            try
            {
                wakeUpWorkers(configFile);
                getWorkerStatus();
                System.out.println("Timer is activated!");
                Thread.sleep(1 * 60 * 1000);
            } catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
        }

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
            try
            {
                int port = Integer.parseInt(tokens[2]);
                Socket connection = new Socket(tokens[1], port);
                workers.put(tokens[0], new WorkerConnection(connection, tokens[0]));
            } catch (IOException ioe)
            {
                System.out.println("Failed To connect to worker " + tokens[0] + " with IP: " + tokens[1]);
                ioe.printStackTrace();
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
        ArrayList<Job> threads = new ArrayList<>();
        Object lock = new Object();
        for (WorkerConnection connection : workers.values())
        {
            Job job = new Job(connection, 0);
            threads.add(job);
            job.start();
        }

        for (Job job : threads)
        {
            try
            {
                job.join();
            } catch (InterruptedException ie)
            {
                ie.printStackTrace();
            }
        }
    }
}
