package distributed;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;


// Temporary solution.
class Job extends Thread
{
    private final int mode;
    private WorkerConnection connection;
    private final Object lock;

    public Job(WorkerConnection connection, int mode, Object lock)
    {
        this.lock = lock;
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
        synchronized (lock)
        {
            System.out.println("Worker stats: " + status);
        }
        connection.close();
    }
}

public class WorkerManager extends Thread
{
    private String configFile;
    private HashMap<String, WorkerConnection> workers;

    public WorkerManager(String configPath)
    {
        configFile = configPath;
        workers = new HashMap<>();
    }

    @Override
    public void run()
    {
        wakeUpWorkers(configFile);
        getWorkerStatus();
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

    /**
     * This method gets the CPU, RAM status of each worker.
     */
    public void getWorkerStatus()
    {
        ArrayList<Job> threads = new ArrayList<>();
        Object lock = new Object();
        for (WorkerConnection connection : workers.values())
        {
            Job job = new Job(connection, 0, lock);
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
