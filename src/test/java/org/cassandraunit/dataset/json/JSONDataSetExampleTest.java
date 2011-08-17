package org.cassandraunit.dataset.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import me.prettyprint.hector.api.ddl.ColumnType;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.schema.JsonSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author Jeremy Sevellec
 *
 */
public class JSONDataSetExampleTest {

	private Logger log = LoggerFactory.getLogger(JSONDataSetExampleTest.class);

	@Test
	public void shouldGenerateAJSONDataSetDocument() throws JAXBException {

		JSONKeyspace keyspace = new JSONKeyspace();
		keyspace.setName("beautifulKeyspaceName");
		keyspace.setReplicationFactor(1);
		List<JSONColumnFamily> columnFamilies = keyspace.getColumnFamilies();

		JSONColumnFamily columnFamily1 = new JSONColumnFamily();
		columnFamily1.setName("columnFamily1");
		columnFamily1.setType(ColumnType.STANDARD);
		columnFamily1.setKeyType(JSONDataType.UTF8Type);
		columnFamily1.setComparatorType(JSONDataType.UTF8Type);
		columnFamily1.setDefaultColumnValueType(JSONDataType.UTF8Type);
		List<JSONRow> rows1 = new ArrayList<JSONRow>();
		JSONRow row1 = new JSONRow();
		row1.setKey("key01");
		List<JSONColumn> columns1 = row1.getColumns();
		JSONColumn column1 = new JSONColumn();
		column1.setName("columnName1");
		column1.setValue("columnValue1");
		columns1.add(column1);
		rows1.add(row1);
		columnFamily1.setRows(rows1);

		columnFamilies.add(columnFamily1);

		JSONColumnFamily columnFamily2 = new JSONColumnFamily();
		columnFamily2.setName("columnFamily1");
		columnFamily2.setType(ColumnType.SUPER);
		columnFamily2.setKeyType(JSONDataType.UTF8Type);
		columnFamily2.setComparatorType(JSONDataType.UTF8Type);
		columnFamily2.setDefaultColumnValueType(JSONDataType.UTF8Type);
		List<JSONRow> rows2 = new ArrayList<JSONRow>();
		JSONRow row2 = new JSONRow();
		row2.setKey("key02");
		List<JSONSuperColumn> superColumns = row2.getSuperColumns();
		JSONSuperColumn superColumn2 = new JSONSuperColumn();
		superColumn2.setName("superColumnName2");
		List<JSONColumn> columns2 = superColumn2.getColumns();
		JSONColumn column2 = new JSONColumn();
		column2.setName("columnName2");
		column2.setValue("columnValue2");
		columns2.add(column2);
		superColumns.add(superColumn2);
		rows2.add(row1);
		columnFamily2.setRows(rows2);
		columnFamilies.add(columnFamily2);

		ObjectMapper jSONMapper = new ObjectMapper();
		try {
			JsonSchema schema = jSONMapper.generateJsonSchema(JSONKeyspace.class);
			log.debug(schema.toString());
			log.debug(jSONMapper.writeValueAsString(keyspace));
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
