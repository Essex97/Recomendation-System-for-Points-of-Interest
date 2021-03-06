/**
 * Created by
 * Marios Prokopakis(3150141)
 * Stratos Xenouleas(3150130)
 * Foivos Kouroutsalidis(3080250)
 * Dimitris Staratzis(3150166)
 * To set the memory used by the JVM in Intellij Alt+Shift+F10 -> Edit Configuration -> VM options: -Xmx2000m
 */
package distributed;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;
import java.util.Arrays;

public class Worker
{

    /**
     * Prints the max memory used by the JVM
     */
    private static String mb(long s)
    {
        return String.format("%d (%.2f M)", s, (double) s / (1024 * 1024));
    }

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private ServerSocket providerSocket;
    private Socket connection;
    private int numberOfProcessors, from, to, k;
    private String RamCpuStats;
    private double l;// normalization factor
    private RealMatrix C, P;

    /**
     * This is the default constructor of the Worker class.
     */
    public Worker()
    {
        this.out = null;
        this.in = null;
        this.providerSocket = null;
        this.connection = null;
        this.numberOfProcessors = 0;
        RamCpuStats = null;
        C = null;
        P = null;
    }

    public static void main(String args[])
    {
        new Worker().startWorker();
    }

    /**
     * The method below starts each worker as a server and remains on hold until it connects with a Master
     */
    private void startWorker()
    {
        try
        {
            providerSocket = new ServerSocket(4201, 10);
            System.out.println("Worker started");

            // Accept the connection
            connection = providerSocket.accept();
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());


            while (true)
            {
                try
                {
                    String msg = (String) in.readObject();
                    if (msg.equals("status"))
                    {
                        refreshStatistics(); //this method call rebalances the system
                        out.writeObject(RamCpuStats);
                        out.flush();
                    } else if (msg.equals("init"))
                    {
                        P = (RealMatrix) in.readObject();
                        C = (RealMatrix) in.readObject();
                        k = (Integer) in.readObject();
                        l = (Double) in.readObject();
                    } else if (msg.equals("trainX"))
                    {
                        trainX();
                    } else if (msg.equals("trainY"))
                    {
                        trainY();
                    }
                } catch (ClassNotFoundException cnfe)
                {
                    cnfe.printStackTrace();
                }
            }


        } catch (IOException ioException)
        {
            ioException.printStackTrace();
        } finally
        {
            try
            {
                out.close();
                providerSocket.close();
            } catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }
    }

    /**
     * This method trains the X matrix
     */
    private void trainX()
    {
        try
        {
            RealMatrix Y = (RealMatrix) in.readObject();

            from = (Integer) in.readObject();
            to = (Integer) in.readObject();

            System.out.println("size: " + (to - from+1));
            RealMatrix X = MatrixUtils.createRealMatrix(to - from +1, k);

            RealMatrix I = MatrixUtils.createRealIdentityMatrix(k);
            RealMatrix I2 = MatrixUtils.createRealIdentityMatrix(C.getColumnDimension());

            RealMatrix Y_T = Y.transpose();

            for (int j = 0; j < X.getRowDimension(); j++)  //For each user
            {

                RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(C.getRow(j + from));
                RealMatrix temp1 = Y_T.multiply(Y).add(Y_T.multiply(Cu.subtract(I2)).multiply(Y));
                RealMatrix temp = temp1.add(I.scalarMultiply(l));
                RealMatrix temp1Inverse = new LUDecomposition(temp).getSolver().getInverse();
                RealMatrix temp2 = Y_T.multiply(Cu).multiply(P.getRowMatrix(j + from).transpose());
                RealMatrix Xu = temp1Inverse.multiply(temp2);
                X.setRowMatrix(j, Xu.transpose());
            }
            out.writeObject(X.copy());
            out.flush();

        } catch (IOException io)
        {
            io.printStackTrace();
        } catch (ClassNotFoundException cnf)
        {
            cnf.printStackTrace();
        }
    }

    /**
     * This method trains the Y matrix
     */
    private void trainY()
    {
        try
        {
            RealMatrix X = (RealMatrix) in.readObject();

            from = (Integer) in.readObject();
            to = (Integer) in.readObject();
            System.out.println("sizeY: " + (to - from+1));
            RealMatrix Y = MatrixUtils.createRealMatrix(to - from +1, k);

            RealMatrix I = MatrixUtils.createRealIdentityMatrix(k);
            RealMatrix I3 = MatrixUtils.createRealIdentityMatrix(C.getRowDimension());

            RealMatrix X_T = X.transpose();
            for (int j = 0; j < Y.getRowDimension(); j++)    //For each poi
            {

                RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(C.getColumn(j + from));
                RealMatrix temp1 = X_T.multiply(X).add(X_T.multiply(Ci.subtract(I3)).multiply(X));
                RealMatrix temp = temp1.add(I.scalarMultiply(l));
                RealMatrix temp1Inverse = new LUDecomposition(temp).getSolver().getInverse();
                RealMatrix temp2 = X_T.multiply(Ci).multiply(P.getColumnMatrix(j + from));
                RealMatrix Yi = temp1Inverse.multiply(temp2);
                Y.setRowMatrix(j, Yi.transpose());
            }
            out.writeObject(Y.copy());
            out.flush();

        } catch (IOException io)
        {
            io.printStackTrace();
        } catch (ClassNotFoundException cnf)
        {
            cnf.printStackTrace();
        }
    }

    /**
     * This method refreshes the statistics for each Worker. This procedure is useful in order to rebalance the system.
     */
    private void refreshStatistics()
    {
        numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
        RamCpuStats = String.valueOf(Runtime.getRuntime().totalMemory() / 10000000) + ";" + String.valueOf(numberOfProcessors * 100);
    }
}
