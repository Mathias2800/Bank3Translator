
package bankthreetranslator;


import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import dk.cphbusiness.connection.ConnectionCreator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import utilities.xml.xmlMapper;

/**
 *
 * @author Mathias
 */
public class BankThreeTranslator {

    private static final String REPLY_QUEUE = "bank_three_normalizer_gr1";
    private static final String EXCHANGE_NAME = "ex_translators_gr1";
    private static final String QUEUE_NAME = "bank_three_gr1";
    private static final String[] TOPICS = {"cheap.*" , "expensive.*"};

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        Channel channelIn = creator.createChannel();
        Channel channelOut = creator.createChannel();
        channelIn.queueDeclare(QUEUE_NAME, true, false, false, null);
        channelIn.exchangeDeclare(EXCHANGE_NAME, "topic");

        for (String topic : TOPICS) {
            channelIn.queueBind(QUEUE_NAME, EXCHANGE_NAME, topic);
        }

        QueueingConsumer consumer = new QueueingConsumer(channelIn);
        channelIn.basicConsume(QUEUE_NAME, true, consumer);


        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = translateMessage(delivery);
            System.out.println(message);
            BasicProperties probs = new BasicProperties.Builder().replyTo(REPLY_QUEUE).correlationId("1").build();
            channelOut.basicPublish("", QUEUE_NAME, probs, message.getBytes());
        }
    }

    private static String translateMessage(QueueingConsumer.Delivery delivery) {
        String message = new String(delivery.getBody());
        XPath xPath = XPathFactory.newInstance().newXPath();
        Document doc = xmlMapper.getXMLDocument(message);
        try {
            String ssn = xPath.compile("/LoanRequest/ssn").evaluate(doc);
            ssn = ssn.replace("-", "");
            doc.getElementsByTagName("ssn").item(0).getFirstChild().setNodeValue(ssn);
        } catch (XPathExpressionException ex) {
            Logger.getLogger(BankThreeTranslator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return xmlMapper.getStringFromDoc(doc);
    }
}
