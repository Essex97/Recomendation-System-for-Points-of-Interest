package distributed;

import org.apache.commons.math3.linear.OpenMapRealMatrix;
import java.util.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Master
{
    private ServerSocket server;
    private ArrayList<WorkerConnection> workers;
    OpenMapRealMatrix POIS;

    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        workers = new ArrayList<WorkerConnection>();
        POIS = MatrixFactorization.readFile("resources/input_matrix_no_zeros.csv");
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
        workers.sort((WorkerConnection l, WorkerConnection r) ->
        {
            return r.getComputerScore() - l.getComputerScore();
        });
        System.out.println(POIS.getRowDimension() + " MALAKAS ");
        train();
        for (WorkerConnection a : workers)
        {
            System.out.println(a.getName());
        }

        //DEBUG
        /*for(int i=0; i<POIS.getRowDimension(); i++)
        {
            for(int j=0; j<POIS.getColumnDimension(); j++)
            {
                System.out.print(POIS.getEntry(i, j) + " ");
            }
            System.out.println();
        }*/
        listenForConnections();
    }

    /**
     * This method initializes the workers ArrayList with a
     * WorkerConnection instance for each worker that is
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
                        workers.add(new WorkerConnection(connection, tokens[0]));
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
        for (WorkerConnection connection : workers)
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
    }

    /**
     * This method sends parts of the array
     * that contains the POIS to the workers
     * and requests them to start the training.
     */
    public void train()
    {
        ArrayList<Thread> threads = new ArrayList<Thread>();
        int step = POIS.getRowDimension() / workers.size();
        int from = 0, to = step;

        for (WorkerConnection connection : workers)
        {
            int Lfrom = from, Lstep = step;
            Thread job = new Thread(() ->
            {

                WorkerConnection con = connection;
                con.sendData("train");
                OpenMapRealMatrix data = new OpenMapRealMatrix(step, POIS.getColumnDimension());
                synchronized (POIS)
                {
                    for (int i = 0; i < Lstep; i++)
                    {
                        for (int j = 0; j < data.getColumnDimension(); j++)
                        {
                            data.setEntry(i, j, POIS.getEntry(Lfrom + i, j));
                        }
                    }
                }
                con.sendData(data);
                OpenMapRealMatrix alteredData = (OpenMapRealMatrix) con.readData();

                synchronized (POIS)
                {
                    for (int i = 0; i < Lstep; i++)
                    {
                        for (int j = 0; j < data.getColumnDimension(); j++)
                        {
                            POIS.setEntry(Lfrom + i, j, alteredData.getEntry(i, j));
                        }
                    }
                    System.out.println(Thread.currentThread().getName());
                }

            });
            threads.add(job);
            job.start();
            from = to;
            to += step;
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
