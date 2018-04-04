package distributed;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Master
{
    private ServerSocket server;
    private ArrayList<WorkerConnection> workers;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    int k;
    private OpenMapRealMatrix POIS;
    private RealMatrix X, C, P, Y;


    /**
     * This is the default constructor of the Master class.
     */
    public Master()
    {
        k = 100;
        workers = new ArrayList<WorkerConnection>();
        POIS = readFile();
        X = MatrixUtils.createRealMatrix(POIS.getRowDimension(), k);
        Y = MatrixUtils.createRealMatrix(POIS.getColumnDimension(), k);
        P = MatrixUtils.createRealMatrix(POIS.getRowDimension(), POIS.getColumnDimension());
        C = MatrixUtils.createRealMatrix(POIS.getRowDimension(), POIS.getColumnDimension());
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

        manageWorkLoad();

        initiallizeMatrices();

        for (WorkerConnection a : workers)
        {
            System.out.println(a.getName() + " " + a.getWorkLoadPercentage());
        }

        train();
       /* for (WorkerConnection a : workers)
        {
            System.out.println(a.getName());
        }*/

        //DEBUG
        /*for (int i = 0; i < POIS.getRowDimension(); i++)
        {
            for (int j = 0; j < POIS.getColumnDimension(); j++)
            {
                System.out.print(POIS.getEntry(i, j) + " ");
            }
            System.out.println();
        }
        */
        listenForConnections();
    }

    /**
     *
     */
    private void initiallizeMatrices()
    {
        // Initialize the tables X, Y randomly first
        for (int i = 0; i < X.getRowDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                X.setEntry(i, j, Math.random());
            }
        }

        for (int i = 0; i < Y.getColumnDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                Y.setEntry(i, j, Math.random());
            }
        }


        //We create the binary Table P which has 1 at the points where the table POIS have value > 0 and 0 anywhere else
        for (int i = 0; i < POIS.getRowDimension(); i++)
        {
            for (int j = 0; j < POIS.getColumnDimension(); j++)
            {
                if (POIS.getEntry(i, j) > 0)
                {
                    P.setEntry(i, j, 1);
                } else
                {
                    P.setEntry(i, j, 0);
                }
            }
        }


        //Creation of C table where C(u,i) = 1 + a * P(u,i)
        for (int i = 0; i < POIS.getRowDimension(); i++)
        {
            for (int j = 0; j < POIS.getColumnDimension(); j++)
            {
                C.setEntry(i, j, 1 + 40 * P.getEntry(i, j));
            }
        }

        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (WorkerConnection connection : workers)
        {
            Thread job = new Thread(() ->
            {
                WorkerConnection con = connection;
                con.sendData("init");
                con.sendData(P);
                con.sendData(C);

            });
            threads.add(job);
            job.start();
        }
        //join
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
     * This method indicates the computing power difference between workers
     */
    private void manageWorkLoad()
    {
        int totalScore = 0;
        for (WorkerConnection c : workers)
        {
            totalScore += c.getComputerScore();
        }
        for (WorkerConnection d : workers)
        {
            d.setWorkLoadPercentage(((double) d.getComputerScore() / (double) (totalScore)));
        }

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
       /* if (workers.size() > 0)
            step = POIS.getRowDimension() / workers.size();*/
        int from = 0, to = 0;

        for (WorkerConnection connection : workers)
        {
            int Lfrom = from, Lstep = (int) ((double) POIS.getRowDimension() * connection.getWorkLoadPercentage());
            to = Lfrom + Lstep;
            int Lto = to;
            Thread job = new Thread(() ->
            {

                WorkerConnection con = connection;
                con.sendData("train");
                OpenMapRealMatrix data;
                if (workers.size() > 1 && connection == workers.get(workers.size() - 1) && Lto < POIS.getRowDimension())
                {
                    data = new OpenMapRealMatrix(Lstep + POIS.getRowDimension() - Lto, POIS.getColumnDimension());
                } else
                {
                    data = new OpenMapRealMatrix(Lstep, POIS.getColumnDimension());
                }

                synchronized (POIS)
                {
                    for (int i = 0; i < Lstep; i++)
                    {
                        for (int j = 0; j < data.getColumnDimension(); j++)
                        {
                            data.setEntry(i, j, POIS.getEntry(Lfrom + i, j));
                        }
                    }
                    //assign remaining POIS to the last worker
                    if (workers.size() > 1 && connection == workers.get(workers.size() - 1))
                    {
                        for (int i = 0; i < POIS.getRowDimension() - Lto; i++)
                        {
                            System.out.println("malakas " + Lto + " " + POIS.getRowDimension());
                            for (int j = 0; j < data.getColumnDimension(); j++)
                            {
                                data.setEntry(i + Lstep, j, POIS.getEntry(i, j));
                            }
                        }
                    }
                }
                con.sendData(data);
                OpenMapRealMatrix alteredData = (OpenMapRealMatrix) con.readData();


                //place altered data to original array
                synchronized (POIS)
                {
                    for (int i = 0; i < data.getRowDimension(); i++)
                    {
                        for (int j = 0; j < data.getColumnDimension(); j++)
                        {
                            POIS.setEntry(Lfrom + i, j, alteredData.getEntry(i, j));
                        }
                    }
                }

            });
            threads.add(job);
            job.start();
            from = to;
        }
        //join threads
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
                ClientConnection con = new ClientConnection(client);
                con.start();
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
     * This method reads the data set from the file and return it.
     */
    public static OpenMapRealMatrix readFile()
    {

        int columnsNum = 1964; //number of dataset's columns
        int rowsNum = 765;    // number of dataset's rows

        BufferedReader br;
        FileReader fr;
        String line;
        int i, j;         //row, column index
        double value;    // value of (i,j)

        try
        {
            fr = new FileReader("resources/input_matrix_no_zeros.csv");
            br = new BufferedReader(fr);

            OpenMapRealMatrix sparse_m = new OpenMapRealMatrix(rowsNum, columnsNum);

            while ((line = br.readLine()) != null)
            {
                String[] splited = line.split(",");

                i = Integer.parseInt(splited[0].trim());
                j = Integer.parseInt(splited[1].trim());
                value = Double.parseDouble(splited[2].trim());

                sparse_m.addToEntry(i, j, value);
            }

            br.close();

            return sparse_m;

        } catch (IOException e)
        {

            e.printStackTrace();

        }
        return null;
    }
}
