package distributed;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import java.io.*;

public class MatrixFactorization
{
    public static void main(String args[])
    {
        OpenMapRealMatrix pois = readFile();

        train(pois);
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


    /**
     * This method trains the tables X and Y to be able
     * to predict user preferences.
     *
     * @param pois This is the table containing the data.
     */
    private static void train(OpenMapRealMatrix pois)
    {
        int k = 100;
        RealMatrix X = MatrixUtils.createRealMatrix(pois.getRowDimension(), k);
        RealMatrix Y = MatrixUtils.createRealMatrix(pois.getColumnDimension(), k);


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

        RealMatrix C = MatrixUtils.createRealMatrix(pois.getRowDimension(), pois.getColumnDimension());

        for (int i = 0; i < pois.getRowDimension(); i++)
        {
            for (int j = 0; j < pois.getColumnDimension(); j++)
            {
                C.setEntry(i, j, 1 + 40 * P.getEntry(i, j));
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

        //Create 3 one dimensional matrices which has only aces. They will help us at the calculations
        double[] once = new double[k];
        double[] once1 = new double[pois.getColumnDimension()];
        double[] once2 = new double[pois.getRowDimension()];

        for (int i = 0; i < once.length; i++) { once[i] = 1; }

        for (int i = 0; i < once1.length; i++) { once1[i] = 1; }

        for (int i = 0; i < once2.length; i++) { once2[i] = 1; }

        //Assign the above Tables at the diagonal and multiply the fist one with l
        RealMatrix I = MatrixUtils.createRealDiagonalMatrix(once);
        double l = 0.01;  //normalization constant
        RealMatrix I1 = I.scalarMultiply(l);

        RealMatrix I2 = MatrixUtils.createRealDiagonalMatrix(once1);
        RealMatrix I3 = MatrixUtils.createRealDiagonalMatrix(once2);


        for (int e = 0; e < 10; e++)
        { //For each epoch

            final long startTime = System.currentTimeMillis();

            //Training of X

            RealMatrix Y_T = Y.transpose();
            for (int j = 0; j < pois.getRowDimension(); j++)
            { //For each user

                RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(C.getRow(j));
                RealMatrix temp1 = Y_T.multiply(Y).add(Y_T.multiply(Cu.subtract(I2)).multiply(Y)).add(I1);
                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();
                RealMatrix temp2 = Y_T.multiply(Cu).multiply(P.getRowMatrix(j).transpose());
                RealMatrix Xu = temp1Inverse.multiply(temp2);
                //System.out.println("Xu :"+j);
                X.setRowMatrix(j, Xu.transpose());
            }

            //Training of Y

            RealMatrix X_T = X.transpose();
            for (int j = 0; j < pois.getColumnDimension(); j++)
            { //For each poi

                RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(C.getColumn(j));
                RealMatrix temp1 = X_T.multiply(X).add(X_T.multiply(Ci.subtract(I3)).multiply(X)).add(I1);
                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();
                RealMatrix temp2 = X_T.multiply(Ci).multiply(P.getColumnMatrix(j));
                RealMatrix Yi = temp1Inverse.multiply(temp2);
                //System.out.println("Yi :"+j);
                Y.setRowMatrix(j, Yi.transpose());
            }

            calculateCost(pois, X, Y, P, C, l, e);

            final long endTime = System.currentTimeMillis();

            System.out.println("Total execution time: " + (endTime - startTime) +"ms" );
        }

    }


    /**
     * This method calculates the cost of each epoch.
     *
     * @param pois This is the table containing the data.
     * @param X This is the trained table X (for each user).
     * @param Y This is the trained table Y (for each poi).
     * @param P This is the binary table P.
     * @param C The normalizing border which originated from pois.
     * @param l This is normalization constant.
     * @param e The current epoch.
     *
     */
    private static void calculateCost(RealMatrix pois, RealMatrix X, RealMatrix Y, RealMatrix P, RealMatrix C, double l, int e){
        double cost = 0;

        for (int u = 0; u < pois.getRowDimension(); u++)
        {
            for (int i = 0; i < pois.getColumnDimension(); i++)
            {
                double c = P.getEntry(u, i) - X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getColumn(0)[0];
                cost += C.getEntry(u, i) * Math.pow(c, 2);
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
        System.out.println(e +" cost : " + cost);
    }

}