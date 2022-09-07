package com.jacobr4d.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.mapreduce.master.MasterServer;
import com.jacobr4d.mapreduce.worker.WorkerServer;

public class Launcher {
	private static final Logger logger = LogManager.getLogger(Launcher.class);
	
	public static void main(String args[]) throws IOException {
				
		if (args.length != 4) {
            logger.info("Usage: Launcher [masterIp] [masterPort] [workerPort] [workerStorageDir]");
            System.exit(1);
        }

		new MasterServer(args[0]);
        new WorkerServer(args[1], args[2], args[3]);
        logger.info("One master server and one local server launched on localhost");
        logger.info("Press [Enter] to shut down this node...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		System.exit(0);
	}
}
