package distributed;
// To set the memory used by the JVM in Intellij Alt+Shift+F10 -> Edit Configuration -> VM options: -Xmx2000m

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.io.*;
import java.net.*;

public class Worker
{
    // Used to print max memory used by the JVM
    static String mb(long s)
    {
        return String.format("%d (%.2f M)", s, (double) s / (1024 * 1024));
    }

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private int numberOfProcessors, from, to, k;
    private String RamCpuStats;
    private double l;
    private RealMatrix C, P;

    public static void main(String args[])
    {
        new Worker().startWorker();
    }

    private ServerSocket providerSocket;
    private Socket connection = null;

    public void startWorker()
    {
        try
        {
            providerSocket = new ServerSocket(6666, 10);
            // Accept the connection
            connection = providerSocket.accept();
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());

            System.out.println("New connection..");
            System.out.println("Runtime max memory: " + mb(Runtime.getRuntime().maxMemory()));

            numberOfProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
            RamCpuStats = String.valueOf(Runtime.getRuntime().maxMemory()) + ";" + String.valueOf(numberOfProcessors);

            // I added the while loop to test the master/worker communication.
            while (true)
            {
                try
                {
                    String msg = (String) in.readObject();
                    if (msg.equals("status"))
                    {
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
                    //pass
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

    private void trainX()
    {
        try
        {
            RealMatrix Matrix = (RealMatrix) in.readObject();

            /*for(int i = 0; i < Matrix.getRowDimension(); i++){
                for (int j = 0; j < Matrix.getColumnDimension(); j++){
                    System.out.print(Matrix.getEntry(i, j)+" ");
                }
                System.out.println();
            }*/

            from = (Integer) in.readObject();
            to = (Integer) in.readObject();
            RealMatrix X = MatrixUtils.createRealMatrix(to - from, k);
            System.out.println("diastaseis " + (to - from));

            double[] once = new double[k];
            double[] once1 = new double[C.getColumnDimension()];
            for (int i = 0; i < once.length; i++)
            {
                once[i] = 1;
            }
            for (int i = 0; i < once1.length; i++)
            {
                once1[i] = 1;
            }
            RealMatrix I = MatrixUtils.createRealDiagonalMatrix(once);
            RealMatrix I1 = I.scalarMultiply(l);
            RealMatrix I2 = MatrixUtils.createRealDiagonalMatrix(once1);

            RealMatrix Y_T = Matrix.transpose();

            for (int j = 0; j < to - from; j++)
            { //For each user
               /* if(j==0)
                    System.out.println("training in progress ");*/
                RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(C.getRow(j+from));
                RealMatrix temp1 = Y_T.multiply(Matrix).add(Y_T.multiply(Cu.subtract(I2)).multiply(Matrix)).add(I1);
                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();
                RealMatrix temp2 = Y_T.multiply(Cu).multiply(P.getRowMatrix(j+from).transpose());

                for(int i = 0; i < X.getColumnDimension(); i++){
                    System.out.print(X.getEntry(j, i)+" ");
                }

                System.out.println();

                RealMatrix Xu = temp1Inverse.multiply(temp2);
                //System.out.println("Xu :" + Xu);
                X.setRowMatrix(j, Xu.transpose());

                for(int i = 0; i < X.getColumnDimension(); i++){
                    System.out.print(X.getEntry(j, i)+" ");
                }

                System.out.println();

            }
            System.out.println("training finished ");
            out.writeObject(X);
            out.flush();

        } catch (IOException io)
        {
            System.out.println("io exception");
        } catch (ClassNotFoundException cnf)
        {
            System.out.println("cnf exception");
        }


    }

    private void trainY()
    {
        try
        {
            RealMatrix Matrix = (RealMatrix) in.readObject();

            /*for(int i = 0; i < Matrix.getRowDimension(); i++){
                for (int j = 0; j < Matrix.getColumnDimension(); j++){
                    System.out.print(Matrix.getEntry(i, j)+" ");
                }
                System.out.println();
            }*/


            from = (Integer) in.readObject();
            to = (Integer) in.readObject();
            RealMatrix Y = MatrixUtils.createRealMatrix(to - from, k);

            double[] once = new double[k];
            double[] once2 = new double[C.getRowDimension()];

            for (int i = 0; i < once.length; i++)
            {
                once[i] = 1;
            }

            for (int i = 0; i < once2.length; i++)
            {
                once2[i] = 1;
            }

            //Assign the above Tables at the diagonal and multiply the fist one with l
            RealMatrix I = MatrixUtils.createRealDiagonalMatrix(once);
            RealMatrix I1 = I.scalarMultiply(l);
            RealMatrix I3 = MatrixUtils.createRealDiagonalMatrix(once2);


            RealMatrix X_T = Matrix.transpose();
            for (int j = 0; j < to-from; j++)
            { //For each poi

                RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(C.getColumn(j+from));
                RealMatrix temp1 = X_T.multiply(Matrix).add(X_T.multiply(Ci.subtract(I3)).multiply(Matrix)).add(I1);
                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();
                RealMatrix temp2 = X_T.multiply(Ci).multiply(P.getColumnMatrix(j+from));

                for(int i = 0; i < Y.getColumnDimension(); i++){
                    System.out.print(Y.getEntry(j, i)+" ");
                }

                System.out.println();

                RealMatrix Yi = temp1Inverse.multiply(temp2);
                //System.out.println("Yi :"+j);
                Y.setRowMatrix(j, Yi.transpose());

                for(int i = 0; i < Y.getColumnDimension(); i++){
                    System.out.print(Y.getEntry(j, i)+" ");
                }

                System.out.println();
            }


            System.out.println("training finished ");
            out.writeObject(Y);
            out.flush();

        } catch (IOException io)
        {
            System.out.println("io exception");
        } catch (ClassNotFoundException cnf)
        {
            System.out.println("cnf exception");
        }


    }

}
