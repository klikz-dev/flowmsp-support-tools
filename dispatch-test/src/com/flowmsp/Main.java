package com.flowmsp;

import com.amazonaws.services.simpleemail.model.GetCustomVerificationEmailTemplateRequest;
import com.flowmsp.domain.customer.Customer;
import com.flowmsp.service.Message.MessageResult;
import com.flowmsp.service.Message.MessageService;
import com.flowmsp.service.MessageParser.MsgParser;
import com.flowmsp.service.MessageParser.ParsedMessage;
import com.flowmsp.service.MessageParser.ParserFactory;
import com.flowmsp.service.pubsub.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import com.flowmsp.service.pubsub.googlemailresult;
//import com.flowmsp.db.CustomerDao;
import com.flowmsp.domain.customer.Customer;


import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.printf("Usage: %s [from-email] [parser-name] [message-id]\n", "doug");
            System.exit(1);
        }

        var from_email = args[0];
        var parser_name = args[1];
        var message_id = args[2];
    System.out.printf("from_email=<%s>\n", from_email);
    System.out.printf("parser=<%s>\n", parser_name);
    System.out.printf("message_id=<%s>\n", message_id);

    // get all emails for customer
    // test each email against chosen parser
    // report on results
        var credentials = new googlecredentials();
        credentials.clientID = System.getenv("CLIENT_ID");
        credentials.clientSecret = System.getenv("CLIENT_SECRET");
        credentials.refreshToken = System.getenv("REFRESH_TOKEN");

        var authorization = new googleauthorization(credentials);

        System.out.printf("clientID=<%s>\n", credentials.clientID);
        System.out.printf("clientSecret=<%s>\n", credentials.clientSecret);
        System.out.printf("refreshToken=<%s>\n", credentials.refreshToken);

        var URL = String.format("https://www.googleapis.com/gmail/v1/users/me/messages?q=from:%s",
            from_email);

        //var URL = "https://www.googleapis.com/gmail/v1/users/me/messages?q=from:hiplink@ecom911dispatch.com";

        // var URL = https://www.googleapis.com/gmail/v1/users/me/messages/16bb4bd08103cbf3

        googlepubsub.GetMyInstance(credentials).Initialize();

        var jb = pubsubhttp.GetMyInstance().PerfromGET(URL);

        JSONArray msgs = (JSONArray) jb.get("messages");

        System.out.printf("found %d messages", msgs.size());

        var mail = new googlemail();

        var messageService = new MessageService(
                null,
                null,
                null,
                null,
                null,
                null);
        var msgParser = ParserFactory.CreateObject("email", parser_name);
        var cust = new Customer();

    //        for (int i = 0; i < msgs.size(); i++) {

        for (int i = 0; i < 10; i++) {
            var m = (JSONObject)msgs.get(i);
            var id = m.get("id").toString();

            System.out.printf("%3d. id=%s\n", i, id);

            var mailmsg = mail.GetEmail(id);
            System.out.printf("subject=<%s>\n", mailmsg.Subject);

            MessageResult newRow = new MessageResult();
            newRow.messageID = mailmsg.messageID;
            newRow.emailGateway = mailmsg.From;
            newRow.messageRaw = mailmsg.Body;

            newRow.customer = cust;
            var plainMsg = newRow.messageRaw;
            var result = msgParser.Process(cust, plainMsg, messageService);

            System.out.printf("ErrorFlag=%d\n", result.ErrorFlag);
        }

        System.exit(0);


            /*
            slug = getSlugfromEmailID(emailGateway, message);

            if (slug != customer_id) continue;

            newRow.customer = cust;
            MsgParser msgParser = ParserFactory.CreateObject("email", cust.emailFormat);
            String plainMsg = newRow.messageRaw;
            ParsedMessage parsedMessage = msgParser.Process(cust, plainMsg, this);
            if (parsedMessage.ErrorFlag != 0) {
                log.error("Error parsing Email Location-1:" + plainMsg);
            }
            newRow.messageRefined = parsedMessage.text;
            newRow.messageType = parsedMessage.Code;
            newRow.messageAddress = parsedMessage.Address;
            newRow.messageLocation = parsedMessage.location;
            newRow.messageLatLon = parsedMessage.messageLatLon;
             */
    }
}
