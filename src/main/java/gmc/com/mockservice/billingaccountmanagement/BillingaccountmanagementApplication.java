package gmc.com.mockservice.billingaccountmanagement;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;
import java.util.Random;

@SpringBootApplication
public class BillingaccountmanagementApplication {
	public static KafkaProducer<String, String> producer = null;

	public static void main(String[] args) throws  Exception {
		SpringApplication.run(BillingaccountmanagementApplication.class, args);

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
		Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

		String[] enrollementNames = {"PG", "OY", "HX", "HH", "AA"};
		Random random = new Random();


		while (true) {

			for (String enrollmentId : enrollementNames) {

				double ifSkip = random.nextInt(10);

				if (enrollmentId.equals("PG") && ifSkip < 2) continue;
				if (enrollmentId.equals("OY") && ifSkip < 4) continue;
				if (enrollmentId.equals("HX") && ifSkip < 6) continue;
				if (enrollmentId.equals("HH") && ifSkip < 8) continue;
				if (enrollmentId.equals("AA") && ifSkip < 1) continue;

				int accountId = 0;

				if (enrollmentId.equals("PG")) accountId = random.nextInt(30) + 100;
				if (enrollmentId.equals("OY")) accountId = random.nextInt(30) + 110;
				if (enrollmentId.equals("HX")) accountId = random.nextInt(30) + 120;
				if (enrollmentId.equals("HH")) accountId = random.nextInt(30) + 130;
				if (enrollmentId.equals("AA")) accountId = random.nextInt(30) + 140;

				ProducerRecord<String, String> record = new ProducerRecord<>("onstar-profile", enrollmentId, String.valueOf(accountId));

				producer.send(record, (RecordMetadata r, Exception e) -> {
					if (e != null) {
						System.out.println("Error producing to topic " + r.topic());
						e.printStackTrace();
					}
				});

				Thread.sleep(300);
			}
		}
	}

	@Bean
	public KafkaConsumerListener messageListener() {
		return new KafkaConsumerListener();
	}

}
