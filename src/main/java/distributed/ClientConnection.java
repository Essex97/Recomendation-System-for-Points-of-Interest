/**
 * Created by
 * Marios Prokopakis(3150141)
 * Stratos Xenouleas(3150130)
 * Foivos Kouroutsalidis(3080250)
 * Dimitris Staratzis(3150166)
 */
package distributed;

import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.*;


class ArrayIndexComparator implements Comparator<Integer>
{
    private final Double[] array;

    /**
     * This is the constructor of the ArrayIndexComparator class
     *
     * @param array This is the array that we use to sort the indexes accordingly
     */
    public ArrayIndexComparator(Double[] array)
    {
        this.array = array;
    }

    /**
     * This method creates the index array
     */
    public Integer[] createIndexArray()
    {
        Integer[] indexes = new Integer[array.length];
        for (int i = 0; i < array.length; i++)
        {
            indexes[i] = i;
        }
        return indexes;
    }

    @Override
    /**
     * This method sorts the index array based on the comparator
     */
    public int compare(Integer index1, Integer index2)
    {
        return array[index2].compareTo(array[index1]);
    }
}

public class ClientConnection extends Thread
{
    private Socket client;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private RealMatrix predictions;


    /**
     * This is the constructor of the ClientConnections class
     *
     * @param connection  the socket between the client and Master
     * @param predictions this is the array which was created after training
     */
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

    /**
     * This method returns the row which contains the predictions for a specific user
     *
     * @param id The user
     */
    public synchronized double[] getUserPredictionWithId(int id)
    {
        return predictions.getRow(id);
    }


    @Override
    /**
     * This method starts the thread
     */
    public void run()
    {
        try
        {

            String a = (String) in.readObject();

            String[] tokens = a.split(";");
            int id = Integer.parseInt(tokens[1]);
            int topK = Integer.parseInt(tokens[0]);
            double locationLat = Double.parseDouble(tokens[2]);
            double locationLongt = Double.parseDouble(tokens[3]);
            int kilometers = Integer.parseInt(tokens[4]);
            int category  = Integer.parseInt(tokens[5]);
            POIS poiLocation = new POIS();
            poiLocation.setLatitude(locationLat);
            poiLocation.setLongtitude(locationLongt);
            System.out.println("Message from client to Master: " + id + " " + topK + " ");
            double[] b = getUserPredictionWithId(id);
            Double[] c = new Double[b.length];
            for (int i = 0; i < b.length; i++)
            {
                c[i] = new Double(b[i]);
            }

            ArrayIndexComparator comparator = new ArrayIndexComparator(c);
            Integer[] indexes = comparator.createIndexArray();
            Arrays.sort(indexes, comparator);
            Integer[] topKIndexes = new Integer[topK];
            POIS[] poisInfo = new POIS[topK];
            int i=0;
            int j=0;
            if(category==0){
                while(i<topK && j<indexes.length-1)
                {
                    if((CalculationByDistance(Master.POISinfo[indexes[j]], poiLocation) <= kilometers)  && !isPOIVisitedbyClient(indexes[j], id))
                    {
                        topKIndexes[i] = indexes[j];
                        i++;
                    }
                    j++;

                }
            }else if(category==1)
            {
                while(i<topK && j<indexes.length-1)
                {
                    if((CalculationByDistance(Master.POISinfo[indexes[j]], poiLocation) <= kilometers) &&(Master.POISinfo[indexes[j]].getCategory().equals("Bars"))  && !isPOIVisitedbyClient(indexes[j], id))
                    {
                        topKIndexes[i] = indexes[j];
                        i++;
                    }
                    j++;

                }
            }else if(category==2)
            {
                while(i<topK && j<indexes.length-1)
                {
                    if((CalculationByDistance(Master.POISinfo[indexes[j]], poiLocation) <= kilometers) &&(Master.POISinfo[indexes[j]].getCategory().equals("Food"))  && !isPOIVisitedbyClient(indexes[j], id))
                    {
                        topKIndexes[i] = indexes[j];
                        i++;
                    }
                    j++;

                }
            }else if(category==3)
            {
                while(i<topK && j<indexes.length-1)
                {
                    if((CalculationByDistance(Master.POISinfo[indexes[j]], poiLocation) <= kilometers) &&(Master.POISinfo[indexes[j]].getCategory().equals("Arts & Entertainment"))  && !isPOIVisitedbyClient(indexes[j], id))
                    {
                        topKIndexes[i] = indexes[j];
                        i++;
                    }
                    j++;

                }
            }


            out.writeObject(topKIndexes);
            out.flush();

            for(int k = 0; k<topK; k++)
            {
                poisInfo[k] = Master.POISinfo[topKIndexes[k]];
            }
            out.writeObject(poisInfo);
            out.flush();


            out.writeObject(poiLocation);
            out.flush();

        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException cnfe)
        {

        } finally
        {
            close();
        }
    }

    /**
     * This method calculates the distance between two pois
     */
    public  double CalculationByDistance(POIS StartP, POIS EndP) {
        int Radius = 6371;// radius of earth in Km
        double lat1 = StartP.getLatitude();
        double lat2 = EndP.getLatitude();
        double lon1 = StartP.getLongtitude();
        double lon2 = EndP.getLongtitude();
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2)
                * Math.sin(dLon / 2);
        double c = 2 * Math.asin(Math.sqrt(a));
        double valueResult = Radius * c;
        double km = valueResult / 1;
        DecimalFormat newFormat = new DecimalFormat("####");
        int kmInDec = Integer.valueOf(newFormat.format(km));
        double meter = valueResult % 1000;
        int meterInDec = Integer.valueOf(newFormat.format(meter));
        return Radius * c;
    }

    /**
     * This method returns true if a poi has been visited by a user or the other way around
     * @param poi the poi
     * @param user the user
     */
    public boolean isPOIVisitedbyClient(int poi, int user)
    {
        if(Master.POIS.getColumn(poi)[user] !=0 )
        {
            return true;
        }
        return false;
    }

    /**
     * This method is used to close all streams
     * and connections that are related to this manager.
     */
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
