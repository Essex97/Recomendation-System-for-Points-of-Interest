package distributed;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.io.*;
import java.net.*;

public class Client
{

    public static void main(String args[])
    {
        new Client().connectToMaster();
    }

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



            out.writeObject("10;459");
            out.flush();
            Integer[] topKIndexes = (Integer[])in.readObject();
            for (int i = 0; i<topKIndexes.length; i++)
            {
                System.out.println(topKIndexes[i]);
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
