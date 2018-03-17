import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Master {
    private ServerSocket workerSocket;
    private Socket workerConnection;
    private ObjectOutputStream out;

    public static void main(String[] args) {
        new Master().startMaster();
    }

    public void startMaster() {
        try {
            workerSocket = new ServerSocket(4200,10);

            workerConnection=workerSocket.accept();//perimenei gia thn sindesh

            out = new ObjectOutputStream(workerConnection.getOutputStream());
            String data = "HELLO WORLD";
            out.writeObject(data);
            out.flush();


        } catch (Exception e) {

            System.out.println("Socket closed");

        } finally {
            try {
                out.close();
                workerConnection.close();
                workerSocket.close();
            }
            catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }

    }
}
