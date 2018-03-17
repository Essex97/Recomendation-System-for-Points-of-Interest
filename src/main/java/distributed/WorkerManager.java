package distributed;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class WorkerManager
{
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public boolean isOk()
    {
        return isOk;
    }

    private String name;
    private Socket connection;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private boolean isOk;

    /**
     * This is the constructor of the WorkerManager class.
     *
     * @param con  This is the connection with the worker.
     * @param name This is the name of the worker as read
     *             form the worker.config file.
     */
    public WorkerManager(Socket con, String name)
    {
        this.isOk = true;
        this.connection = con;
        this.name = name;
        this.in = null;
        this.out = null;
        try
        {
            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());
        } catch (IOException ioe)
        {
            isOk = false;
            ioe.printStackTrace();
        }
    }

    /**
     * This method is used to send an object to the other
     * end of the connection.
     *
     * @param obj The object to be send.
     */
    public void sendData(Object obj)
    {
        try
        {
            out.writeObject(obj);
        } catch (IOException ioe)
        {
            isOk = false;
            ioe.printStackTrace();
        }
    }

    /**
     * This method is used to read an object
     * from the other end of the connection.
     */
    public Object readData()
    {
        Object ret = null;
        try
        {
            ret = in.readObject();
        } catch (IOException ioe)
        {
            isOk = false;
            ioe.printStackTrace();
        } catch (ClassNotFoundException cnfe)
        {
            isOk = false;
            cnfe.printStackTrace();
        }

        return ret;
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
            connection.close();
        } catch (IOException ioe)
        {
            isOk = false;
            ioe.printStackTrace();
        }
    }
}
