/**
 * Created by
 * Marios Prokopakis(3150141)
 * Stratos Xenouleas(3150130)
 * Foivos Kouroutsalidis(3080250)
 * Dimitris Staratzis(3150166)
 */
package distributed;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class Client
{

    public static void main(String args[])
    {
        new Client().connectToMaster();
    }

    /**
     * This method connects to Master using a requestSocket
     */
    public void connectToMaster()
    {
        Socket requestSocket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try
        {

            /* Create socket for contacting the server on port 7777*/
            requestSocket = new Socket("localhost", 7777);
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
            out.writeObject("11;479");
            out.flush();

            Integer[] topKIndexes = (Integer[])in.readObject();
            POIS[] poisInfo = (POIS[])in.readObject();
            for (int i = 0; i<topKIndexes.length; i++)
            {
                System.out.println(topKIndexes[i] + " KENO " +poisInfo[i]);
            }



        } catch (UnknownHostException unknownHost)
        {
            System.err.println("Unknown host");
        } catch (IOException ioException)
        {
            ioException.printStackTrace();
        } catch (ClassNotFoundException cnfe)
        {

        } finally
        {
            try
            {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }

    }
}
