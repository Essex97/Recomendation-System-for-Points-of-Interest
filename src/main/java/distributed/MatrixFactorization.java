package distributed;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.*;
import java.util.ArrayList;

public class MatrixFactorization
{


    public static void main(String args[])
    {
        OpenMapRealMatrix pois = readFile("resources/input_matrix_no_zeros.csv");

        train(pois);
    }


    //-----------------ReadTestTable---------------------------------------------------//
    public static OpenMapRealMatrix readFile(String path)
    {

        int columnsNum = 1964; //sthlh
        int rowsNum = 765;    // seira

        BufferedReader br;
        FileReader fr;
        String line;
        int i, j;         //row, column index
        double value;    // value of (i,j)

        try
        {
            fr = new FileReader(path);
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

    //-----------------TrainTable---------------------------------------------------//
    private static void train(OpenMapRealMatrix pois)
    {

        // Calculate the dimension of X, Y

        int k = (pois.getRowDimension() * pois.getColumnDimension()) / (pois.getRowDimension() + pois.getColumnDimension());
        System.out.println(k);
        k = 100;
        RealMatrix X = MatrixUtils.createRealMatrix(pois.getRowDimension(), k);
        RealMatrix Y = MatrixUtils.createRealMatrix(pois.getColumnDimension(), k);


        // Initialize the tables X, Y randomly first

        //RandomGenerator randomGenerator = new JDKRandomGenerator();
        //randomGenerator.setSeed(1);

        for (int i = 0; i < X.getRowDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                //X.setEntry(i, j, Math.floor(randomGenerator.nextDouble() * 100) / 100);
                X.setEntry(i, j, Math.random());
            }
        }

        for (int i = 0; i < Y.getColumnDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                //Y.setEntry(i, j, Math.floor(randomGenerator.nextDouble() * 100) / 100);
                Y.setEntry(i, j, Math.random());
            }
        }


        //We create the binary Table P which has 1 at the points where the table POIs have value > 0 and 0 anywhere else

        RealMatrix P = MatrixUtils.createRealMatrix(pois.getRowDimension(), pois.getColumnDimension());

        for (int i = 0; i < pois.getRowDimension(); i++)
        {
            for (int j = 0; j < pois.getColumnDimension(); j++)
            {
                if (pois.getEntry(i, j) > 0)
                {
                    P.setEntry(i, j, 1);
                } else
                {
                    P.setEntry(i, j, 0);
                }
            }
        }


        //Creation of Cui table where C(u,i) = 1 + a * P(u,i)

        RealMatrix Cui = MatrixUtils.createRealMatrix(pois.getRowDimension(), pois.getColumnDimension());
        int a = 40;
        for (int i = 0; i < pois.getRowDimension(); i++)
        {
            for (int j = 0; j < pois.getColumnDimension(); j++)
            {
                Cui.setEntry(i, j, 1 + a * P.getEntry(i, j));
            }
        }


        /*//Keep all the tables Cu which are needed to train the X Table at an ArrayList called CuRefernces

        ArrayList<OpenMapRealMatrix> CuRefernces = new ArrayList<>();

        for (int l = 0; l < pois.getRowDimension(); l++)
        {
            RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(Cui.getRow(l));
            Cu = Cu.scalarMultiply(-1.0);
            OpenMapRealMatrix CuOpen = new OpenMapRealMatrix(pois.getColumnDimension(), pois.getColumnDimension());
            CuOpen = CuOpen.subtract(Cu);
            CuRefernces.add(CuOpen);                  // We have the Cu Tables for each user
            System.out.println("Cu :" + l);
        }*/


        /*//Keep all the tables Ci which are needed to train the Y Table at an ArrayList called CiRefernces

        ArrayList<OpenMapRealMatrix> CiRefernces = new ArrayList<>();

        for (int l = 0; l < pois.getColumnDimension(); l++)
        {
            RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(Cui.getColumn(l));
            Ci = Ci.scalarMultiply(-1.0);
            OpenMapRealMatrix CiOpen = new OpenMapRealMatrix(pois.getRowDimension(), pois.getRowDimension());
            CiOpen = CiOpen.subtract(Ci);
            CiRefernces.add(CiOpen);                 // We have the Ci Tables for each user
            System.out.println("Ci :" + l);
        }*/

        //Create an one dimensional matrix which has only aces
        double[] once = new double[k];
        for (int i = 0; i < once.length; i++)
        {
            once[i] = 1;
        }

        //Assign the above Table at the diagonal and multiply it with l
        RealMatrix I = MatrixUtils.createRealDiagonalMatrix(once);
        double l = 0.01;
        RealMatrix I1 = I.scalarMultiply(l);



        double[] once1 = new double[pois.getColumnDimension()];
        for (int i = 0; i < once1.length; i++)
        {
            once1[i] = 1;
        }
        RealMatrix I2 = MatrixUtils.createRealDiagonalMatrix(once1);



        double[] once2 = new double[pois.getRowDimension()];
        for (int i = 0; i < once2.length; i++)
        {
            once2[i] = 1;
        }
        RealMatrix I3 = MatrixUtils.createRealDiagonalMatrix(once2);




        for (int e = 0; e < 10; e++)
        { //For each epoch

            final long startTime = System.currentTimeMillis();

            RealMatrix Y_T = Y.transpose();

            for (int j = 0; j < pois.getRowDimension(); j++)
            { //For each user

                final long startTime1 = System.currentTimeMillis();

                RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(Cui.getRow(j));

                RealMatrix temp1 = Y_T.multiply(Y);

                RealMatrix afairesh = Cu.subtract(I2);

                RealMatrix temp2 = Y_T.multiply(afairesh);

                RealMatrix temp3 = temp2.multiply(Y);

                RealMatrix temp4 = temp1.add(temp3);

                RealMatrix temp5 = temp4.add(I1);

                RealMatrix temp5Inverse = new QRDecomposition(temp5).getSolver().getInverse();

                RealMatrix temp6 = Y_T.multiply(Cu);

                RealMatrix temp7 = temp6.multiply(P.getRowMatrix(j).transpose());

                RealMatrix Xu = temp5Inverse.multiply(temp7);


                System.out.println("Xu :"+j);

                X.setRowMatrix(j, Xu.transpose());

                final long endTime1 = System.currentTimeMillis();

                System.out.println("Total execution time: " + (endTime1 - startTime1) +"\n");

            }

            RealMatrix X_T = X.transpose();

            for (int j = 0; j < pois.getColumnDimension(); j++)
            { //For each poi

                final long startTime2 = System.currentTimeMillis();

                RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(Cui.getColumn(j));

                RealMatrix temp1 = X_T.multiply(X);

                RealMatrix temp2 = X_T.multiply(Ci.subtract(I3));

                RealMatrix temp3 = temp2.multiply(X);

                RealMatrix temp4 = temp1.add(temp3);

                RealMatrix temp5 = temp4.add(I1);

                RealMatrix temp5Inverse = new QRDecomposition(temp5).getSolver().getInverse();

                RealMatrix temp6 = X_T.multiply(Ci);

                RealMatrix temp7 = temp6.multiply(P.getColumnMatrix(j));

                RealMatrix Yi = temp5Inverse.multiply(temp7);


                System.out.println("Yi :"+j);

                Y.setRowMatrix(j, Yi.transpose());

                final long endTime2 = System.currentTimeMillis();

                System.out.println("Total execution time: " + (endTime2 - startTime2) +"\n");

            }

            double cost = 0;

            for (int u = 0; u < pois.getRowDimension(); u++)
            {
                for (int i = 0; i < pois.getColumnDimension(); i++)
                {
                    double c = P.getEntry(u, i);

                    double c1 = X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getColumn(0)[0];

                    cost += Cui.getEntry(u, i) * Math.pow(c - c1, 2);

                    System.out.println(cost);
                }
            }

            int Xsum = 0;
            int Ysum = 0;

            for (int u = 0; u < pois.getRowDimension(); u++)
            {
                Xsum += Math.pow(X.getRowMatrix(u).getNorm(), 2);
            }

            for (int i = 0; i < pois.getRowDimension(); i++)
            {
                Ysum += Math.pow(Y.getRowMatrix(i).getNorm(), 2);
            }

            cost += l * (Xsum + Ysum);
            System.out.println("cost : " + cost);

            final long endTime = System.currentTimeMillis();

            System.out.println("Total execution time: " + (endTime - startTime)/1000 );
        }

    }

}