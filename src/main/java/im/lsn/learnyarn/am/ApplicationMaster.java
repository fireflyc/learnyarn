package im.lsn.learnyarn.am;

public class ApplicationMaster {
    public static void main(String[] args) throws InterruptedException {
        while (true) {
            System.out.println("(stdout)Hello World");
            System.err.println("(stderr)Hello World");
            Thread.sleep(1000);
        }
    }

}
