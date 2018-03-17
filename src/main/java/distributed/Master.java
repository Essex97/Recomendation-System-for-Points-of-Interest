package distributed;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

public class Master
{
    private HashMap<String, WorkerManager> workers;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        workers = new HashMap<>();
    }

    /**
     * This method initializes the workers map with a pair
     * of <worker name, WorkerManager> for each worker that is
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
        }finally
        {
           try
           {
               br.close();
           }catch (IOException ioe)
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
                workers.put(tokens[0], new WorkerManager(connection, tokens[0]));
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
    }

    /**
     * This method gets the CPU, RAM status of each worker.
     */
    public void getWorkerStatus()
    {
        for (WorkerManager manager : workers.values())
        {
            String status = (String) manager.readData();
            System.out.println(status);
            manager.close();
        }
    }
}
