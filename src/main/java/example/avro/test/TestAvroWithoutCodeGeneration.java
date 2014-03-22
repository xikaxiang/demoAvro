package example.avro.test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class TestAvroWithoutCodeGeneration {

	public static void main(String[] args) throws IOException {
		// serialize();
		deserialize();
	}

	public static void deserialize() throws IOException {

		String userDir = System.getProperty("user.dir");

		File avscFile = new File(userDir + "/src/main/avro/user.avsc");

		if (avscFile.exists()) {
			Schema schema = new Parser().parse(avscFile);
			// Deserialize users from disk
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
					schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
					new File("users2.avro"), datumReader);
			GenericRecord user = null;

			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println(user);

			}
		}

	}

	public static void serialize() {
		try {

			String userDir = System.getProperty("user.dir");

			File avscFile = new File(userDir + "/src/main/avro/user.avsc");
			if (avscFile.exists()) {
				Schema schema = new Parser().parse(avscFile);
				// Using this schema, let's create some users.
				GenericRecord user1 = new GenericData.Record(schema);
				user1.put("name", "BAlyssa");
				user1.put("favorite_number", 1256);
				// Leave favorite color null

				GenericRecord user2 = new GenericData.Record(schema);
				user2.put("name", "BBen");
				user2.put("favorite_number", 17);
				user2.put("favorite_color", "red");

				File file = new File("users2.avro");
				DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
						schema);
				DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
						datumWriter);
				dataFileWriter.create(schema, file);
				dataFileWriter.append(user1);
				dataFileWriter.append(user2);
				dataFileWriter.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 查看所有默认配置属性及其属性值
	public static void seekSysProperties() {
		Map evn = System.getProperties();
		Properties props = System.getProperties();
		Set set = props.keySet();
		Iterator iterator = set.iterator();
		while (iterator.hasNext()) {
			Object key = iterator.next();
			Object value = props.get(key);
			System.out.println(key + " : " + value);
		}
	}
}
