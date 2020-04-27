package be.catsandcoding.pairprogramming.server;
import org.zeromq.*;

public class HwServer {

    public static void main(String[] args) throws Exception {
        Thread mainAction = new Thread(new server_task());
        mainAction.start();
        while(mainAction.isAlive()){
            Thread.sleep(200);
        }

    }

    private static class server_task implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                ZMQ.Socket frontend = ctx.createSocket(SocketType.ROUTER);
                frontend.bind("tcp://*:5570");

                ZMQ.Socket backend = ctx.createSocket(SocketType.DEALER);
                backend.bind("inproc://backend");

                ZMQ.Socket publish = ctx.createSocket((SocketType.PUB));
                publish.bind("tcp://*:5571");

                for (int threadNbr = 0; threadNbr < 5; threadNbr++)
                    new Thread(new server_worker(ctx, publish)).start();

                ZMQ.proxy(frontend, backend, null);
            }
        }
    }

    private static class server_worker implements Runnable {
        private final ZContext ctx;
        private final ZMQ.Socket pub;

        public server_worker(ZContext ctx,ZMQ.Socket pub) {
            this.ctx = ctx;
            this.pub = pub;
        }

        @Override
        public void run() {
            ZMQ.Socket worker = ctx.createSocket(SocketType.DEALER);
            worker.connect("inproc://backend");

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg msg = ZMsg.recvMsg(worker);
                ZFrame address = msg.pop();
                ZFrame content = msg.pop();
                System.out.println(String.format("received {%s} {%s}", address, content));
                assert (content != null);
                msg.destroy();

                System.out.println("sending: " + content.toString());
                pub.send(content.toString());
                address.destroy();
                content.destroy();
            }
            ctx.destroy();
        }
    }
}

