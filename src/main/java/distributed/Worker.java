package distributed;

import java.io.ObjectInputStream;
import java.net.Socket;

// test comment
public class Worker {

    private Socket masterConnection;
    private ObjectInputStream in;
    public static void main(String [] args)

    {
        new Worker().startWorker();
    }

    public void startWorker()
    {
        try
        {
            masterConnection = new Socket("localhost", 4200);
            in = new ObjectInputStream(masterConnection.getInputStream());
            String toPrint = (String)in.readObject();
            System.out.println(toPrint);
        }
        catch(Exception e)
        {

        }
        finally
        {
            try
            {
                in.close();
                masterConnection.close();
            }
            catch(Exception e)
            {

            }


        }
    }
}
