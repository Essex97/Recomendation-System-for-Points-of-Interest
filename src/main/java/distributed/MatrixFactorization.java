package distributed;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class MatrixFactorization {

    public static void main(String args[]) {

        writeTable("Table.txt");

        OpenMapRealMatrix pois;
        pois = readFile("Table.txt");

        //System.out.println(pois);

        train(pois);
    }

    //--------------------WriteTestTable--------------------------------------------//

    public static void writeTable(String fileName) {

        FileWriter fileWriter = null;
        Random rand = new Random();

        int columnsNum = 100; //sthlh
        int rowsNum = 100; // seira

        try {
            fileWriter = new FileWriter(fileName);

            for (int i = 0; i < rowsNum; i++) {
                for (int j = 0; j < columnsNum; j++) {

                    if (Math.random() < 0.5) {
                        fileWriter.append(0 + " ");
                    } else {
                        fileWriter.append(Math.abs(rand.nextInt() % 100) + " ");
                    }
                }
                fileWriter.append('\n');
            }

        } catch (Exception e) {

            System.out.println("Error in CsvFileWriter.");
            e.printStackTrace();

        } finally {

            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter.");
                e.printStackTrace();
            }

        }
    }


    //-----------------ReadTestTable---------------------------------------------------//
    public static OpenMapRealMatrix readFile(String path) {

        int columnsNum = 100; //sthlh
        int rowsNum = 100; // seira

        BufferedReader br = null;
        FileReader fr = null;

        try {
            fr = new FileReader(path);
            br = new BufferedReader(fr);

            OpenMapRealMatrix sparse_m = new OpenMapRealMatrix(rowsNum, columnsNum);

            for (int i = 0; i < rowsNum; i++) {

                String line = br.readLine();
                String[] tokens = line.split(" ");

                for (int j = 0; j < columnsNum; j++) {
                    sparse_m.setEntry(i, j, Double.parseDouble(tokens[j]));
                }
            }

            return sparse_m;

        } catch (IOException e) {

            e.printStackTrace();

        }
        return null;
    }

    //-----------------TrainTable---------------------------------------------------//
    public static void train(OpenMapRealMatrix pois){

       // Concluse that the pois Table has size M x N
        int k = 20; //random k
        RealMatrix X = MatrixUtils.createRealMatrix(pois.getRowDimension(),k);
        RealMatrix Y = MatrixUtils.createRealMatrix(pois.getColumnDimension(),k);

        RandomGenerator randomGenerator =  new JDKRandomGenerator();
        randomGenerator.setSeed(23);

        for(int i =0 ; i< X.getRowDimension();i++){
            for(int j = 0 ; j< X.getColumnDimension();j++){
                X.setEntry(i,j,randomGenerator.nextDouble());
            }
        }

        for(int i =0 ; i< Y.getRowDimension();i++){
            for(int j = 0 ; j< Y.getColumnDimension();j++){
                Y.setEntry(i,j,randomGenerator.nextDouble());
            }
        }

        // Now we have the random matrixes L, R. We want

        RealMatrix P = MatrixUtils.createRealMatrix(pois.getRowDimension(), pois.getColumnDimension());
        for(int i =0 ; i< pois.getRowDimension();i++){
            for(int j = 0 ; j< pois.getColumnDimension();j++){

                if (pois.getEntry(i, j) > 0){
                    P.setEntry(i,j,1);
                }else {
                    P.setEntry(i,j,0);
                }

            }
        }

        // Now we have the table P witch is 0 everyWhere an 1 at the point where the table POIs hava value > 1

        for(int i = 0; i < 20; i++){

            ArrayList <RealMatrix> Crefernces = new ArrayList <RealMatrix>();
            for(int l = 0;  l < pois.getRowDimension(); k++){
                //Crefernces.add(MatrixUtils.createRealDiagonalMatrix(pois.getRow(l)));  // We have the diagonals Tables
            }

        }
        //calculateDerivativeX
        //calculateDerivativeY

    }

}