import java.util.*;

public class Main {
    public static void main(String[] args) {
        List<String> l1 = new LinkedList<String>();
        List<String> l2 = new LinkedList<String>();

        l1.add("inputs/0689835604.txt");
        l1.add("inputs/B000EVOSE4.txt");
        l1.add("inputs/B01LYRCIPG.txt");
        l1.add("inputs/B001DZTJRQ.txt");
        l1.add("inputs/B0047E0EII.txt");


        LocalApp app = new LocalApp(l1, l2, 1);

        app.runApp();
    }
}
