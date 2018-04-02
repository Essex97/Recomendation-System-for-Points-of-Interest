package distributed;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ClientConnection extends Thread
{
    private Socket client;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public ClientConnection(Socket connection)
    {
        this.client = connection;
        try
        {
            in = new ObjectInputStream(client.getInputStream());
            out = new ObjectOutputStream(client.getOutputStream());
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        try
        {
            String a = (String) in.readObject();
            System.out.println("Message from client to Master: " + a);
            out.writeObject("TEST MESSAGE 2");
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
