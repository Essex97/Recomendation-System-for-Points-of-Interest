package distributed;

import com.sun.deploy.util.ArrayUtil;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Master
{

    /**
     * This method reads the data set from the file and return it.
     */
    private static OpenMapRealMatrix readFile()
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
                String[] split = line.split(",");

                i = Integer.parseInt(split[0].trim());
                j = Integer.parseInt(split[1].trim());
                value = Double.parseDouble(split[2].trim());

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

    private ServerSocket server;
    private ArrayList<WorkerConnection> workers;
    private int k;
    private double l;
    private OpenMapRealMatrix POIS;
    private RealMatrix X, C, P, Y;
    private RealMatrix predictions;

    /**
     * This is the default constructor of the Master class.
     */
    private Master()
    {
        k = 100;
        l=0.01;
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

        for (WorkerConnection a : workers)
        {
            System.out.println(a.getName() + " " + a.getWorkLoadPercentage());
        }
        initializeMatrices();
        train();
        listenForConnections();
    }

    /**
     * Initialization of the tables we use.
     */
    private void initializeMatrices()
    {
        // Firstly initialize the tables X, Y randomly
        for (int i = 0; i < X.getRowDimension(); i++)
        {
            for (int j = 0; j < X.getColumnDimension(); j++)
            {
                X.setEntry(i, j, Math.random());
            }
        }

        for (int i = 0; i < Y.getRowDimension(); i++)
        {
            for (int j = 0; j < Y.getColumnDimension(); j++)
            {
                Y.setEntry(i, j, Math.random());
            }
        }

        // Creation of the binary Table P whose cells contain the value 1 if the
        // respective POIS cells contain a positive value, and 0 everywhere else.
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
                con.sendData(new Integer(k));
                con.sendData(new Double(l));
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
        for(int e = 0; e < 1; e++){
            trainingEpoch();
            calculateCost();
        }
        predictions = X.multiply(Y.transpose());

        double [] userPref = predictions.getRow(100);

        for(int i = 0; i < userPref.length; i++){
            System.out.println(userPref[i]);
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

                new Thread(()->{
                    Socket con = client;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    try
                    {
                        out = new ObjectOutputStream(con.getOutputStream());
                        in = new ObjectInputStream(con.getInputStream());

                        String input = (String)in.readObject();
                        String [] split = input.split(";");

                        int id = Integer.parseInt(split[0]);
                        int top = Integer.parseInt(split[1]);

                        double [] userPref = predictions.getRow(id);
                        ArrayList <Double> row = new ArrayList<>();

                        for(int i = 0; i < userPref.length; i++){
                            row.add(userPref[i]);
                        }

                        row.sort(Collections.reverseOrder());

                        for(int i =0; i < top; i++){
                            System.out.println(row.get(i));
                        }


                        //out.writeObject(row.subList(0, top-1));
                        //out.flush();


                    }catch (IOException io)
                    {
                        io.printStackTrace();
                    }catch(ClassNotFoundException cnf){
                        cnf.printStackTrace();
                    }
                    finally
                    {
                        try
                        {
                            in.close();
                            out.close();
                            con.close();
                        }catch(IOException io){
                            io.printStackTrace();
                        }

                    }


                }).start();

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
     * This method calculates the cost of each epoch.
     */
    private double calculateCost()
    {
        double cost = 0;

        for (int u = 0; u < POIS.getRowDimension(); u++)
        {
            for (int i = 0; i < POIS.getColumnDimension(); i++)
            {
                double c = P.getEntry(u, i) - X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getColumn(0)[0];
                cost += C.getEntry(u, i) * Math.pow(c, 2);
            }
        }

        int Xsum = 0;
        int Ysum = 0;

        for (int u = 0; u < X.getRowDimension(); u++)
        {
            Xsum += Math.pow(X.getRowMatrix(u).getNorm(), 2);
        }

        for (int i = 0; i < Y.getRowDimension(); i++)
        {
            Ysum += Math.pow(Y.getRowMatrix(i).getNorm(), 2);
        }

        cost += l * (Xsum + Ysum);
        return cost;
    }

    /**
     *
     */
    private void trainingEpoch()
    {
        ArrayList<Thread> threads = new ArrayList<Thread>();
        int from = 0, to;

        for (WorkerConnection connection : workers)
        {
            int Lfrom = from, Lstep = (int) ((double) X.getRowDimension() * connection.getWorkLoadPercentage());
            to = Lfrom + Lstep;
            int Lto = to;
            Thread job = new Thread(() ->
            {
                WorkerConnection con = connection;
                con.sendData("trainX");
                con.sendData(Y.copy());

                if (workers.size() > 1 && connection == workers.get(workers.size() - 1) && Lto < POIS.getRowDimension())
                {
                    con.sendData(new Integer(Lfrom));
                    con.sendData(new Integer(Lto + Lto-POIS.getRowDimension()));
                } else
                {
                    con.sendData(new Integer(Lfrom));
                    con.sendData(new Integer(Lto));
                }
                /*
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
                            for (int j = 0; j < data.getColumnDimension(); j++)
                            {
                                data.setEntry(i + Lstep, j, POIS.getEntry(i, j));
                            }
                        }
                    }
                }
                con.sendData(data);*/

                //data = MatrixUtils.createRealMatrix(Lto -Lfrom, X.getColumnDimension());

                /*for(int i = Lfrom; i < Lto; i++){
                    for(int j = 0; j < X.getColumnDimension(); j++){
                        data.setEntry(i - Lfrom, j, X.getEntry(i, j));
                    }
                }*/


                RealMatrix alteredData = (RealMatrix)con.readData();

                //place altered data to original array
                synchronized (X)
                {
                    for (int i = 0; i < alteredData.getRowDimension(); i++)
                    {
                        for (int j = 0; j < alteredData.getColumnDimension(); j++)
                        {
                            X.setEntry(Lfrom + i, j, alteredData.getEntry(i, j));
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

        // The training o Y matrix starts from here

        threads = new ArrayList<Thread>();
        from = 0;
        to = 0;

        for (WorkerConnection connection : workers)
        {
            int Lfrom = from, Lstep = (int) ((double) Y.getRowDimension() * connection.getWorkLoadPercentage());
            to = Lfrom + Lstep;
            int Lto = to;
            Thread job = new Thread(() ->
            {
                //RealMatrix data;
                WorkerConnection con = connection;
                con.sendData("trainY");

                con.sendData(X.copy());

                if (workers.size() > 1 && connection == workers.get(workers.size() - 1) && Lto < POIS.getRowDimension())
                {
                    con.sendData(new Integer(Lfrom));
                    con.sendData(new Integer(Lto + Lto-POIS.getRowDimension()));
                } else
                {
                    con.sendData(new Integer(Lfrom));
                    con.sendData(new Integer(Lto));
                }

                RealMatrix alteredData = (RealMatrix)con.readData();

                //place altered data to original array
                synchronized (Y)
                {
                    for (int i = 0; i < alteredData.getRowDimension(); i++)
                    {
                        for (int j = 0; j < alteredData.getColumnDimension(); j++)
                        {
                            Y.setEntry(Lfrom + i, j, alteredData.getEntry(i, j));
                        }
                    }
                }
            });
            threads.add(job);
            job.start();
            from = to;
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
}
