package org.rdm.zeromq;

import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZLoop.IZLoopHandler;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZThread.IAttachedRunnable;

import zmq.ZError;
import android.util.Log;

// .split listener thread
// The listener receives all messages flowing through the proxy, on its
// pipe. In CZMQ, the pipe is a pair of ZMQ_PAIR sockets that connect
// attached child threads. In other languages your mileage may vary:
//
// Extend this class and implement handle(Socket, Object). Return -1
// from handle or send anything to pipe to stop listening. Start like
// this:
// Socket pipe = ZThread.fork(ZContext,
//                            ZListener,
//                            ObjectArg);   // [optional] arg to pass
//                                          //            to handle()
public class ZListener implements IAttachedRunnable {
    public String threadName = ZListener.class.getSimpleName();
    public int ops = Poller.POLLIN;

    // Set thread name and return the current instance
    public ZListener name(String threadName) {
        this.threadName = threadName;
        return this;
    }

    // return -1 to quit
    public int handleSocket(Socket socket, Object arg) {
        return printFrame(socket, arg);
    }

    // return -1 to quit
    public int handlePipe(Socket socket, Object arg) {
        return printFrame(socket, arg);
    }

    public Socket setup(Object[] args, ZContext ctx, Socket pipe) {
        // Create and return a socket to poll.
        // Examples:
        //
        //     Socket socket = createAndConnectSocket(ctx, ZMQ.REQ, endpoint);
        //
        //     Socket socket = createAndBindSocket(ctx, ZMQ.SUB, endpoint);
        //     socket.subscribe("".getBytes());
        //
        // Optional:
        //     ops = Poller.POLLIN; // or Poller.POLLOUT;
        //
        // return socket;
        return null;
    }

    // This method is called by run() below. By default, it
    // calls the other setup method without the loop arg.
    public Socket setup(Object[] args, ZContext ctx, Socket pipe, ZLoop loop) {
        return setup(args, ctx, pipe);
    }

    public void log(String message) {
        Log.i(threadName, message);
    }

    public void log(String message, Exception e) {
        Log.i(threadName, message, e);
    }

    int printFrame(Socket socket, Object arg) {
        String msg = socket.recvStr();
        if (msg == null) {
            log("Listener", new Exception());
            return -1;
        }
        log("> " + msg);
        return 0;
    }

    IZLoopHandler socketHandler = new IZLoopHandler() {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            return handleSocket(item.getSocket(), arg);
        }
    };

    IZLoopHandler pipeHandler = new IZLoopHandler() {
        @Override
        public int handle(ZLoop loop, PollItem item, Object arg) {
            return handlePipe(item.getSocket(), arg);
        }
    };

    @Override
    public void run(Object[] args, ZContext ctx, Socket pipe) {
        // args may contain, in order: ThreadName, ObjectArg
        Object arg = null;
        if (args.length > 0 && args[0] != null)
            threadName = (String) args[0];
        if (args.length > 1)
            arg = args[1];
        Thread.currentThread()
              .setName(threadName);
        log(threadName + " starting");
        ZLoop zloop = new ZLoop();
        Socket socket = setup(args, ctx, pipe, zloop);
        if (socket != null)
            zloop.addPoller(new PollItem(socket, ops), socketHandler, arg);
        zloop.addPoller(new PollItem(pipe, Poller.POLLIN), pipeHandler, null);
        try {
            zloop.start();
        } catch (Exception e) {
            log(threadName + " exception", e);
        }
        ctx.destroy();
        log(threadName + " quitting");
    }

    public Socket createAndConnectSocket(ZContext context, int type,
            String endpoint) {
        return Util.createAndConnectSocket(context, type, endpoint, threadName);
    }

    public Socket createAndBindSocket(ZContext context, int type,
            final String endpoint) {
        return Util.createAndBindSocket(context, type, endpoint, threadName);
    }

    public Socket createSocket(ZContext context, int type) {
        return Util.createSocket(context, type, threadName);
    }

    public Socket bindSocket(Socket socket, String address) {
        return Util.bindSocket(socket, address, threadName);
    }

    public static class Util {
        public static Socket createAndConnectSocket(ZContext context, int type,
                String endpoint) {
            return createAndConnectSocket(context, type, endpoint,
                    currentThreadName());
        }

        public static Socket createAndBindSocket(ZContext context, int type,
                final String endpoint) {
            return createAndBindSocket(context, type, endpoint,
                    currentThreadName());
        }

        public static Socket createSocket(ZContext context, int type) {
            return createSocket(context, type, currentThreadName());
        }

        public static Socket bindSocket(Socket socket, String address) {
            return bindSocket(socket, address, currentThreadName());
        }

        ////////////////////////////////////////

        public static Socket createAndConnectSocket(ZContext context, int type,
                final String endpoint, final String tag) {
            Socket socket = createSocket(context, type, tag);
            if (socket != null)
                socket.connect(endpoint);
            return socket;
        }

        public static Socket createAndBindSocket(ZContext context, int type,
                final String endpoint, final String tag) {
            Socket socket = createSocket(context, type);
            if (null == bindSocket(socket, endpoint)) {
                context.destroySocket(socket);
                socket = null;
            }
            return socket;
        }

        public static Socket createSocket(ZContext context, int type,
                final String tag) {
            try {
                return context.createSocket(type);
            } catch (IllegalArgumentException e) {
                // Bad socket type
            } catch (IllegalStateException e) {
                // Maximum number of open 0MQ sockets reached
            } catch (ZError.CtxTerminatedException e) {
                // Context is terminated or terminating
            }
            Log.e(tag, "Failed to create socket");
            return null;

            // Possible errors from zmq_socket(3) man page with
            // corresponding Java errors:
            //
            // EINVAL - The requested socket type is invalid. 
            //     IllegalArgumentException("type=" + type);
            // EFAULT - The provided context is invalid. 
            //     not applicable? I can't find this.
            // EMFILE - The limit on the total number of open 0MQ
            //          sockets has been reached. 
            //     IllegalStateException("EMFILE");
            // ETERM  - The context specified was terminated. 
            //     ZError.CtxTerminatedException()
        }

        public static Socket bindSocket(Socket socket, final String endpoint,
                final String tag) {
            if (socket == null)
                return null;

            while (true)
                try {
                    socket.bind(endpoint);
                    return socket;
                } catch (ZMQException e) {
                    if (e.getErrorCode() != ZError.EADDRINUSE) {
                        Log.e(tag, "Failed to bind socket", e);
                        if (e.getErrorCode() != ZError.ETERM)
                            socket.close();
                        return null;
                    }
                    Log.w(tag, "Address still in use, try again.");
                    sleep(250);
                }
        }

        public static final String currentThreadName() {
            return Thread.currentThread()
                         .getName();
        }

        public static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
            }
        }
    }
}
