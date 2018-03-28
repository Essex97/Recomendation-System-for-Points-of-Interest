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

        //writeTable("resources/Table.txt");

        OpenMapRealMatrix pois = readFile("resources/input_matrix_no_zeros.csv");

        //System.out.println(pois);

        train(pois);
    }

    //--------------------WriteTestTable--------------------------------------------//

    /*public static void writeTable(String fileName)
    {

        FileWriter fileWriter = null;
        Random rand = new Random();

        int columnsNum = 100; //sthlh
        int rowsNum = 100; // seira

        try
        {
            fileWriter = new FileWriter(fileName);

            for (int i = 0; i < rowsNum; i++)
            {
                for (int j = 0; j < columnsNum; j++)
                {

                    if (Math.random() < 0.5)
                    {
                        fileWriter.append(0 + " ");
                    } else
                    {
                        fileWriter.append(Math.abs(rand.nextInt() % 100) + " ");
                    }
                }
                fileWriter.append('\n');
            }

        } catch (Exception e)
        {

            System.out.println("Error in CsvFileWriter.");
            e.printStackTrace();

        } finally
        {

            try
            {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e)
            {
                System.out.println("Error while flushing/closing fileWriter.");
                e.printStackTrace();
            }

        }
    }*/


    //-----------------ReadTestTable---------------------------------------------------//
    private static OpenMapRealMatrix readFile(String path)
    {

        int columnsNum = 1964; //sthlh
        int rowsNum = 765; // seira

        BufferedReader br;
        FileReader fr;
        String line;
        int i, j ;     //row, column index
        double value;  // value of (i,j)


        try
        {
            fr = new FileReader(path);
            br = new BufferedReader(fr);

            OpenMapRealMatrix sparse_m = new OpenMapRealMatrix(rowsNum, columnsNum);

            while((line = br.readLine()) != null){

                String[] splited = line.split(",");

                i = Integer.parseInt(splited[0].trim());
                j = Integer.parseInt(splited[1].trim());
                value = Double.parseDouble(splited[2].trim());

                System.out.println(i +" "+j+" "+value);

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

        // Concluse that the pois Table has size M x N
        int k = (pois.getRowDimension() * pois.getColumnDimension()) / (pois.getRowDimension() + pois.getColumnDimension());

        RealMatrix X = MatrixUtils.createRealMatrix(pois.getRowDimension(), k);
        RealMatrix Y = MatrixUtils.createRealMatrix(pois.getColumnDimension(), k);

        RandomGenerator randomGenerator = new JDKRandomGenerator();
        randomGenerator.setSeed(1);

        for (int i = 0; i < X.getRowDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                X.setEntry(i, j, Math.floor(randomGenerator.nextDouble() * 100) / 100);
            }
        }

        for (int i = 0; i < Y.getColumnDimension(); i++)
        {
            for (int j = 0; j < k; j++)
            {
                Y.setEntry(i, j, Math.floor(randomGenerator.nextDouble() * 100) / 100);
            }
        }

        // Now we have the random matrixes X, Y.

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

        // Now we have the table P witch is 0 everyWhere and 1 at the points where the table POIs hava value > 0

        RealMatrix Cui = MatrixUtils.createRealMatrix(pois.getRowDimension(), pois.getColumnDimension());
        int a = 40;
        for (int i = 0; i < pois.getRowDimension(); i++)
        {
            for (int j = 0; j < pois.getColumnDimension(); j++)
            {

                Cui.setEntry(i, j, 1 + a * P.getEntry(i, j));

            }
        }


        ArrayList<OpenMapRealMatrix> CuRefernces = new ArrayList<>();

        for (int l = 0; l < pois.getRowDimension(); l++)
        {

            RealMatrix Cu = MatrixUtils.createRealDiagonalMatrix(Cui.getRow(l));
            Cu = Cu.scalarMultiply(-1.0);
            OpenMapRealMatrix CuOpen = new OpenMapRealMatrix(pois.getRowDimension(), pois.getColumnDimension());
            CuOpen = CuOpen.subtract(Cu);
            CuRefernces.add(CuOpen);  // We have the Cu Tables for each user

        }

        ArrayList<OpenMapRealMatrix> CiRefernces = new ArrayList<>();

        for (int l = 0; l < pois.getColumnDimension(); l++)
        {

            RealMatrix Ci = MatrixUtils.createRealDiagonalMatrix(Cui.getColumn(l));
            Ci = Ci.scalarMultiply(-1.0);
            OpenMapRealMatrix CiOpen = new OpenMapRealMatrix(pois.getRowDimension(), pois.getColumnDimension());
            CiOpen = CiOpen.subtract(Ci);
            CiRefernces.add(CiOpen);  // We have the Ci Tables for each user

        }

        //System.out.println(CIRefernces.get(0).getRowDimension() + ", " + CIRefernces.get(0).getColumnDimension());

        double[] monades = new double[k];
        for (int i = 0; i < monades.length; i++)
        {
            monades[i] = 1;
        }

        RealMatrix I = MatrixUtils.createRealDiagonalMatrix(monades);
        double l = 0.01;
        I = I.scalarMultiply(l);


        for (int e = 0; e < 10; e++)
        { //For each epoch

            for (int j = 0; j < pois.getRowDimension(); j++)
            { //For each user

                RealMatrix temp1 = Y.transpose().multiply(CuRefernces.get(j)).multiply(Y).add(I);

                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();

                RealMatrix temp2 = Y.transpose().multiply(CuRefernces.get(j)).multiply(P.getRowMatrix(j).transpose());

                //System.out.println(temp2.getRowDimension() + ", " + temp2.getColumnDimension());

                //System.out.println(temp1Inverse.transpose().getRowDimension() + ", " + temp1Inverse.transpose().getColumnDimension());

                RealMatrix Xu = temp1Inverse.multiply(temp2);

                //System.out.println(Xu);

                X.setRowMatrix(j, Xu.transpose());

            }

            for (int j = 0; j < pois.getColumnDimension(); j++)
            {

                RealMatrix temp1 = X.transpose().multiply(CiRefernces.get(j)).multiply(X).add(I);

                RealMatrix temp1Inverse = new QRDecomposition(temp1).getSolver().getInverse();

                RealMatrix temp2 = X.transpose().multiply(CiRefernces.get(j)).multiply(P.getRowMatrix(j).transpose());

                RealMatrix Yi = temp1Inverse.multiply(temp2);

                //System.out.println(Yi.getRowDimension() + ", " + Yi.getColumnDimension());

                Y.setRowMatrix(j, Yi.transpose());
            }

            double cost = 0;

            for (int u = 0; u < pois.getRowDimension(); u++)
            {
                for (int i = 0; i < pois.getColumnDimension(); i++)
                {

                    //System.out.println(X.getRowMatrix(u).transpose().getRowDimension() +" " + X.getRowMatrix(u).transpose().getColumnDimension());

                    //System.out.println(Y.getColumnMatrix(i).getRowDimension() +" " + Y.getColumnMatrix(i).getColumnDimension());


                    //System.out.println(X.getRowMatrix(u).getRowDimension() +" " + X.getRowMatrix(u).getColumnDimension());
                    System.out.println(Y.getRowMatrix(i).transpose().getRowDimension() + " " + Y.getRowMatrix(i).transpose().getColumnDimension());

                    double c = P.getEntry(u, i);
                    //System.out.println(X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()));
                    double c1 = X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getColumn(0)[0];

                    //System.out.println(X.getRowMatrix(u).multiply(Y.getColumnMatrix(i)));

                    //System.out.println(X.getRowMatrix(u)+ " uhjuh ");

                    //System.out.println((Y.getColumnMatrix(i)));

                    cost += Cui.getEntry(u, i) * Math.pow(c - c1, 2);

                }
            }

            int Xsum = 0;
            int Ysum = 0;

            for (int u = 0; u < pois.getRowDimension(); u++)
            {
                Xsum += X.getRowMatrix(u).getFrobeniusNorm();
            }

            for (int i = 0; i < pois.getRowDimension(); i++)
            {
                Ysum += Y.getRowMatrix(i).getFrobeniusNorm();
            }

            cost += l * (Xsum + Ysum);
            System.out.println("cost : " + cost);
        }

    }

}