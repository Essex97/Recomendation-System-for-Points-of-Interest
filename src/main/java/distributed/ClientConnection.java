package distributed;

import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.*;

class ArrayIndexComparator implements Comparator<Integer>{
    private final Double[] array;
    public ArrayIndexComparator(Double[] array){ this.array = array; }

    public Integer[] createIndexArray()
    {
        Integer[] indexes = new Integer[array.length];
        for (int i = 0; i < array.length; i++)
        {
            indexes[i] = i; // Autoboxing
        }
        return indexes;
    }

    @Override
    public int compare(Integer index1, Integer index2)
    {
        // Autounbox from Integer to int to use as array indexes
        return array[index2].compareTo(array[index1]);
    }
}

public class ClientConnection extends Thread
{
    private Socket client;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private RealMatrix predictions;

    public ClientConnection(Socket connection, RealMatrix predictions)
    {
        this.client = connection;
        try
        {
            this.predictions = predictions;
            in = new ObjectInputStream(client.getInputStream());
            out = new ObjectOutputStream(client.getOutputStream());
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    public synchronized double[] getUserPredictionWithId(int id)
    {
        return predictions.getRow(id);
    }


    @Override
    public void run()
    {
        try
        {
            String a = (String) in.readObject();

            String[] tokens = a.split(";");
            int id = Integer.parseInt(tokens[1]);
            int topK = Integer.parseInt(tokens[0]);
            System.out.println("Message from client to Master: " + id + " " + topK);
            double[] b = getUserPredictionWithId(id);
            Double[] c = new Double[b.length];
            for (int i = 0; i<b.length; i++)
            {
                c[i]= new Double(b[i]);
            }

            ArrayIndexComparator comparator = new ArrayIndexComparator(c);
            Integer[] indexes = comparator.createIndexArray();
            Arrays.sort(indexes, comparator);
            Integer[] topKIndexes = new Integer[topK];
            for (int i = 0; i<topK; i++)
            {
                topKIndexes[i] = indexes[i];
            }
            out.writeObject(topKIndexes);
            out.flush();

        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException cnfe)
        {

        } finally
        {
            try
            {
                in.close();
                out.close();
            } catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }
    }


    public void close()
    {
        try
        {
            in.close();
            out.close();
            client.close();
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }
}
