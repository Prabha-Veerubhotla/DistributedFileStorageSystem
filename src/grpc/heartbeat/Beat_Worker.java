package grpc.heartbeat;

import java.lang.*;
import java.util.TimerTask;

public abstract class Beat_Worker extends TimerTask{

    public Beat_Worker(){
        System.out.println("started");
    }

    public final void run(){
        beat();
    }

    protected abstract void beat();
}