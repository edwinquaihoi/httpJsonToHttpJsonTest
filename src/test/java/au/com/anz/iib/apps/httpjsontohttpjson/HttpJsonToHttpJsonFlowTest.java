package au.com.anz.iib.apps.httpjsontohttpjson;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import au.com.anz.json.schema.httpjsontohttpjson.Company;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.broker.config.proxy.AttributeConstants;
import com.ibm.broker.config.proxy.BrokerProxy;
import com.ibm.broker.config.proxy.Checkpoint;
import com.ibm.broker.config.proxy.ConfigManagerProxyLoggedException;
import com.ibm.broker.config.proxy.ConfigManagerProxyPropertyNotInitializedException;
import com.ibm.broker.config.proxy.ExecutionGroupProxy;
import com.ibm.broker.config.proxy.RecordedTestData;

public class HttpJsonToHttpJsonFlowTest {
	
	private static final Logger logger = LogManager.getLogger();
	
	// FIXME make this configurable from system property 
	private static final String BROKER_NODE_NAME = "Node01";
	private static final String INTEGRATION_SERVER_NAME = "svr1";
	
	private static final String TEST_FILE_001 = "HttpJsonToHttpJson.Test001.xml";
	private static BrokerProxy brokerNodeProxy;
	private static ExecutionGroupProxy integrationServerProxy;
	private Gson gson = new Gson();
	
	@BeforeClass
	public static void initialise() throws ConfigManagerProxyLoggedException, ConfigManagerProxyPropertyNotInitializedException {
		// get broker
		brokerNodeProxy = BrokerProxy.getLocalInstance(BROKER_NODE_NAME);
		
		if(brokerNodeProxy != null) {
			if(!brokerNodeProxy.isRunning()) {
				// stop test execution
				fail("Broker Node " + BROKER_NODE_NAME + " is not running. Please start the Node before running Tests.");
			} else {
				// setup integration server reference				
				integrationServerProxy = brokerNodeProxy.getExecutionGroupByName(INTEGRATION_SERVER_NAME);
				
				if(integrationServerProxy != null) {
					// start integration server
					if(!integrationServerProxy.isRunning()) {
						integrationServerProxy.start();
						
						// enable test injection mode
						integrationServerProxy.setInjectionMode(AttributeConstants.MODE_ENABLED);
						integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_ENABLED);
						
						// TODO find a better way to do event handling of asynchronous calls
						// sleep for a second as calls above are asynchronous
						try { Thread.sleep(1000); } catch (InterruptedException e) { }
					}
				} else {
					fail("Integration Server " + INTEGRATION_SERVER_NAME + " is not configured in Broker Node " + BROKER_NODE_NAME + ". Please configure the Integrat before running Tests.");
				}
			}
		}
	}
	
	@AfterClass
	public static void finalise() throws ConfigManagerProxyPropertyNotInitializedException, ConfigManagerProxyLoggedException {
		integrationServerProxy.clearRecordedTestData();
		integrationServerProxy.setInjectionMode(AttributeConstants.MODE_DISABLED);
		integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_DISABLED);
		
	}
	
	@Before
	public void setup() throws ConfigManagerProxyPropertyNotInitializedException, ConfigManagerProxyLoggedException {
		integrationServerProxy.clearRecordedTestData();

		// enable test injection mode
		integrationServerProxy.setInjectionMode(AttributeConstants.MODE_ENABLED);
		integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_ENABLED);
		
		// TODO find a better way to do event handling of asynchronous calls
		// sleep for a second as calls above are asynchronous
		try { Thread.sleep(1000); } catch (InterruptedException e) { }
	}
	
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
		boolean result = integrationServerProxy.injectTestData(injectProps, true);

		// get test data for verification
		Properties filterProps = new Properties();			
		filterProps.put(Checkpoint.PROPERTY_SOURCE_NODE_NAME, "HttpJsonToHttpJson#FCMComposite_1_6");			
		List<RecordedTestData> dataList = integrationServerProxy.getRecordedTestData(filterProps);
		
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
		
		byte[] jsonBlob = DatatypeConverter.parseHexBinary(nl.item(0).getTextContent());
		
		logger.info(new String(jsonBlob));
		
		// turn jsonBlob to GSon
		gson = new Gson();
		List<Company> companies = gson.fromJson(new String(jsonBlob), new TypeToken<List<Company>>(){}.getType());
		
		// assert list contains companies
		assertTrue(companies.size() > 0);
		
		for(Company company : companies) {
			logger.info(company);
		}
	}
}
