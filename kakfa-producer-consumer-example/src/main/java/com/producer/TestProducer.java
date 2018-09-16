package com.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.producer.ProducerCreator;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TestProducer implements Runnable {

	Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	String ck = "Aty8RQgKrgaFwVRNobd6F1xd8";
	String csk = "qRAm8pJTfh56IECzgASeCtDnXWup7ZIPoo5ePkoTOYxzRdc40j";
	String tk = "782602143740076034-Uqf4AZdJJrdrQhZqEOvrg5QdKB7OGMU";
	String tsk = "syEbT8BZYnCuMJei9PPt8ZRQcPXPDR8RFqpc2xnQOjAns";
	BlockingQueue<String> msgQueue = null;

	public TestProducer(BlockingQueue<String> msgQueue) {
		this.msgQueue = msgQueue;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println("Producer Start");
		String filepath = "C:\\test\\SampleCSVFile_556kb.csv";

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		CSVReader reader = new CSVReader(msgQueue, filepath);
		new Thread(reader).start();

		TestProducer producer = new TestProducer(msgQueue);
		new Thread(producer).start();

	}

	public void run() {
		runFile();
	}

	public void runFile() {
		logger.info("SetUp");
		Producer<Long, String> producer = ProducerCreator.createProducer();

		try {
			while (true) {
				String msg = msgQueue.take();
				logger.info(msg);

				ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
						msg);
				try {
					RecordMetadata metadata = producer.send(record).get();
					
				} catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				} catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
				
				if (msg.equals("EOF")) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.info("End of Application");
	}

	public void runTwitter() {
		logger.info("SetUp");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		Client client = createTwitterClient(msgQueue);
		client.connect();

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				client.stop();
			}
			logger.info("5sec");

			if (msg != null) {
				logger.info(msg);
			}
		}

		logger.info("End of Application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("*");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(ck, csk, tk, tsk);

		ClientBuilder builder = new ClientBuilder().name("yuz_test_1") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		// hosebirdClient.connect();

		return hosebirdClient;
	}

}
