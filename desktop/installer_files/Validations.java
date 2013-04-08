import java.net.InetAddress;
import java.net.Socket;

class Validations{
    public static boolean isPortUsed(int port){
        try {
            Socket conn = new Socket(InetAddress.getLocalHost(), port);
            conn.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isPortNumberValid(String portNumber){
        int port = Integer.parseInt(portNumber);
        if (0< port && port<65535){
            return true;
        } else {
            return false;
        }
    }
}