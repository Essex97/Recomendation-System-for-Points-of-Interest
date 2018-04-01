package distributed;

import javax.swing.plaf.synth.SynthOptionPaneUI;
import java.net.ConnectException;
import java.util.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Master
{
    private ServerSocket server;
    private HashMap<String, WorkerConnection> workers;
    private ArrayList<Integer> testArray;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        testArray = new ArrayList<>();
        for (int i = 0; i < 3000; ++i)
        {
            testArray.add(i);
        }

        Collections.shuffle(testArray);

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
        //train();
        //listenForConnections();
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

        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < lines.size(); i++)
        {
            int index = i;
            Thread job = new Thread(() ->
            {
                String[] tokens = lines.get(index).split(" ");
                int port = 0;
                try
                {
                    port = Integer.parseInt(tokens[2]);
                    Socket connection = new Socket(tokens[1], port);
                    synchronized (workers)
                    {
                        workers.put(tokens[0], new WorkerConnection(connection, tokens[0]));
                    }
                    synchronized (threads)
                    {
                        System.out.println("Connected to worker " + tokens[0] + " with IP: " + tokens[1]);
                    }
                } catch (IOException ioe)
                {
                    synchronized (threads)
                    {
                        System.out.println("Failed To connect to worker " + tokens[0] + " with IP: " + tokens[1] + " on port " + port);
                    }
                    //ioe.printStackTrace();
                } catch (NumberFormatException nfe)
                {
                    synchronized (threads)
                    {
                        System.out.println("Can not cast the  ");
                    }
                }
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
                WorkerConnection con = connection;
                con.sendData("status");
                String msg = (String) con.readData();
                //System.out.println(con.getName() + ": " + msg);
                String[] tokens = msg.split(";");
                con.setCpuCores(Integer.parseInt(tokens[1]));
                con.setMemory(Integer.parseInt(tokens[0]));
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
     * This method sends parts of the array
     * that contains the POIS to the workers
     * and requests them to start the training.
     */
    public void train()
    {
        ArrayList<Thread> threads = new ArrayList<Thread>();
        int from = 0, to = 999;
        for (WorkerConnection connection : workers.values())
        {
            int f = from, t = to;
            Thread job = new Thread(() ->
            {
                WorkerConnection con = connection;

                con.sendData("train");
                con.sendData(new ArrayList<>(testArray.subList(f, t)));
                ArrayList<Integer> recved = (ArrayList) con.readData();

                synchronized (testArray)
                {
                    System.out.println(f + " " + " " + t + " finished.");
                    for (int i = 0; i < recved.size(); ++i)
                    {
                        testArray.set(f + i, recved.get(i));
                    }
                }

            });
            threads.add(job);
            job.start();
            from += 999;
            to += 999;
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

        for (Integer i : testArray)
        {
            System.out.println(i);
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
