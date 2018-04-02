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

            

            out.writeObject("TEST MESSAGE");
            out.flush();
            System.out.println("Message from Master to Client: " + in.readObject());



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
