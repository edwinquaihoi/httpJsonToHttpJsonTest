package au.com.anz.iib.apps.httpjsontohttpjson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import au.com.anz.iib.commons.test.FlowTest;
import au.com.anz.json.schema.httpjsontohttpjson.Company;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.broker.config.proxy.AttributeConstants;
import com.ibm.broker.config.proxy.Checkpoint;
import com.ibm.broker.config.proxy.ConfigManagerProxyLoggedException;
import com.ibm.broker.config.proxy.ConfigManagerProxyPropertyNotInitializedException;
import com.ibm.broker.config.proxy.RecordedTestData;

public class HttpJsonToHttpJsonFlowTest extends FlowTest {
	
	private static final Logger logger = LogManager.getLogger();
		
	private static final String TEST_FILE_001 = "HttpJsonToHttpJson.Test001.xml";

	private Gson gson = new Gson();
	
	
	@Test
	public void testMainFlow() throws ConfigManagerProxyPropertyNotInitializedException, ConfigManagerProxyLoggedException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
		
		// load test data from file
		String message = IOUtils.toString(this.getClass().getResourceAsStream(TEST_FILE_001));
		
		logger.info(message);
				
		Properties injectProps = new Properties();
		injectProps.setProperty(AttributeConstants.DATA_INJECTION_APPLICATION_LABEL, "HttpJsonToHttpJson"); 		
		injectProps.setProperty(AttributeConstants.DATA_INJECTION_MESSAGEFLOW_LABEL, "HttpJsonToHttpJson"); 			
		injectProps.setProperty(AttributeConstants.DATA_INJECTION_NODE_UUID, "HttpJsonToHttpJson#FCMComposite_1_1");
		injectProps.setProperty(AttributeConstants.DATA_INJECTION_WAIT_TIME, "60000");
		injectProps.setProperty(AttributeConstants.DATA_INJECTION_MESSAGE_SECTION, "");
		
		// execute flow in sychronous mode
		boolean result = getIntegrationServerProxy().injectTestData(injectProps, true);

		// get test data for verification
		Properties filterProps = new Properties();			
		filterProps.put(Checkpoint.PROPERTY_SOURCE_NODE_NAME, "HttpJsonToHttpJson#FCMComposite_1_6");			
		List<RecordedTestData> dataList = getIntegrationServerProxy().getRecordedTestData(filterProps);
		
		assertEquals(1, dataList.size());

		String outMessage = dataList.get(0).getTestData().getMessage();
		logger.info(outMessage);

		InputStream stream = new ByteArrayInputStream(outMessage.getBytes());
		
		// get the BLOB/BLOB node from the Message Tree
	    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	    DocumentBuilder db = dbf.newDocumentBuilder();
	    	    
	    Document doc = db.parse(stream);
	    //doc.getDocumentElement().normalize();
	    
	    XPathFactory xPathfactory = XPathFactory.newInstance();
	    XPath xpath = xPathfactory.newXPath();
	    XPathExpression expr = xpath.compile("/message/BLOB/BLOB");	    
	    
	    NodeList nl = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
	    
		assertNotNull(nl);		
		assertEquals(1, nl.getLength());
		
		// parse the hex string in the BLOB to a byte array so that it can be read as the original json string
		byte[] jsonBlob = DatatypeConverter.parseHexBinary(nl.item(0).getTextContent());
		
		logger.info(new String(jsonBlob));
		
		// turn jsonBlob to GSon
		List<Company> companies = gson.fromJson(new String(jsonBlob), new TypeToken<List<Company>>(){}.getType());
		
		// assert list contains companies
		assertTrue(companies.size() > 0);		
	}
}
