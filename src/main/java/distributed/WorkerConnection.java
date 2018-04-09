package distributed;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class WorkerConnection
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
    private int cpuCores;
    private int memory;
    private double workLoadPercentage;
    private Socket connection;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private boolean isOk;
    public static final int cpuWeight = 10;
    public static final int memoryWeight = 31;

    public int getCpuCores()
    {
        return cpuCores;
    }

    public void setCpuCores(int cpuCores)
    {
        this.cpuCores = cpuCores;
    }

    public int getMemory()
    {
        return memory;
    }

    public void setMemory(int memory)
    {
        this.memory = memory;
    }

    /**
     * This is the constructor of the WorkerConnection class.
     *
     * @param con  This is the connection with the worker.
     * @param name This is the name of the worker as read
     *             form the worker.config file.
     */
    public WorkerConnection(Socket con, String name)
    {
        this.isOk = true;
        this.memory = 0;
        this.cpuCores = 0;
        this.connection = con;
        this.name = name;
        this.in = null;
        this.out = null;
        try
        {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
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
            out.flush();
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

    public int getComputerScore()
    {
        return cpuCores * cpuWeight + memory * memoryWeight;
    }

    public double getWorkLoadPercentage()
    {
        return workLoadPercentage;
    }

    public void setWorkLoadPercentage(double newPercentage)
    {
        workLoadPercentage = newPercentage;
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
