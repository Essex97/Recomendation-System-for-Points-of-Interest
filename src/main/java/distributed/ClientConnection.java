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
    }

    @Override
    public void run()
    {
        try
        {
            in = new ObjectInputStream(client.getInputStream());
            out = new ObjectOutputStream(client.getOutputStream());
            // TODO: serve the client.
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        } finally
        {
            close();
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
