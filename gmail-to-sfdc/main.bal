import ballerina/log;
import ballerinax/googleapis.gmail as gmail;
import ballerina/mime;
import ballerinax/salesforce as sfdc;
import ballerina/lang.runtime;

type GmailOAuth2Config record {|
    string refreshToken;
    string clientId;
    string clientSecret;
|};

configurable GmailOAuth2Config gmailOAuth2Config = ?;

gmail:ConnectionConfig gmailConfig = {
    auth: {
        refreshUrl: gmail:REFRESH_URL,
        refreshToken: gmailOAuth2Config.refreshToken,
        clientId: gmailOAuth2Config.clientId,
        clientSecret: gmailOAuth2Config.clientSecret
    }
};

type SalesforceOAuth2Config record {|
    string clientId;
    string clientSecret;
    string refreshToken;
    string baseUrl;
    string refreshUrl;
|};

configurable SalesforceOAuth2Config salesforceOAuth2Config = ?;

sfdc:ConnectionConfig sfdcConfig = {
    baseUrl: salesforceOAuth2Config.baseUrl,
    auth: {
        clientId: salesforceOAuth2Config.clientId,
        clientSecret: salesforceOAuth2Config.clientSecret,
        refreshToken: salesforceOAuth2Config.refreshToken,
        refreshUrl: salesforceOAuth2Config.refreshUrl
    }
};

type Email record {|
    string 'from;
    string subject;
    string body;
|};

type Name record {|
    string firstName__c;
    string lastName__c;
|};

type Lead record {|
    *Name;
    string email__c;
    string phoneNumber__c;
    string company__c;
    string designation__c;
|};

public function main() returns error? {
    while true {
        runtime:sleep(10);
        check checkForNewLeads();
    }
}

function checkForNewLeads() returns error? {
    sfdc:Client|error sfdcClient = new (sfdcConfig);
    if sfdcClient is error {
        log:printError("An error occured while initializing the Salesforce client", sfdcClient, sfdcClient.stackTrace());
        return sfdcClient;
    }

    gmail:Client|error gmailClient = new (gmailConfig);
    if gmailClient is error {
        log:printError("An error occured while initializing the GMail client", gmailClient, gmailClient.stackTrace());
        return gmailClient;
    }

    string[] labelsToMatch = ["Lead"];

    string[]|error labelIdsToMatch = getLabelIDs(gmailClient, labelsToMatch);
    if labelIdsToMatch is error {
        log:printError("An error occured while fetching labels", labelIdsToMatch, labelIdsToMatch.stackTrace(), {"labelsToMatch": labelsToMatch});
        return labelIdsToMatch;
    }

    if (labelIdsToMatch.length() == 0) {
        error e = error("Unable to find any labels to match.");
        log:printError("Unable to find any labels to match.", e, e.stackTrace(), {"labelsToMatch": labelsToMatch});
        return e;
    }

    gmail:MsgSearchFilter searchFilter = {
        includeSpamTrash: false,
        labelIds: labelIdsToMatch
    };

    stream<gmail:MailThread, error?>|error mailThreadStream = gmailClient->listThreads(filter = searchFilter);
    if mailThreadStream is error {
        log:printError("An error occured while retrieving the emails.", mailThreadStream, mailThreadStream.stackTrace(), {"searchFilter": searchFilter});
        return mailThreadStream;
    }

    error? e = check from gmail:MailThread thread in mailThreadStream
        limit 1
        do {
            gmail:MailThread|error response = gmailClient->readThread(thread.id);
            if response is error {
                log:printError("An error occured while reading the email.", response, response.stackTrace(), {"threadId": thread.id});
                return response;
            }

            Email|error email = parseEmail((<gmail:Message[]>response.messages)[0]);
            if email is error {
                log:printError("An error occured while parsing the email.", email, email.stackTrace(), {"threadId": thread.id});
            } else {

                Lead|error lead = getLead(email.'from, email.subject, email.body);
                if lead is error {
                    log:printError("An error occured while attempting to generate lead information.", lead, lead.stackTrace(), {"threadId": thread.id, "email": email});
                } else {

                    sfdc:CreationResponse|error createResponse = check sfdcClient->create("EmailLead__c", lead);
                    if createResponse is error {
                        log:printError("An error occured while creating a Lead object on salesforce.", createResponse, createResponse.stackTrace(), {"threadId": thread.id, "email": email, "lead": lead});
                    } else {

                        log:printInfo("Lead successfully created.", Lead = lead);
                        gmail:MailThread|error removeLabelResponse = gmailClient->modifyThread(thread.id, [], labelIdsToMatch);
                        if removeLabelResponse is error {
                            log:printError("An error occured in removing the labels from the thread.", removeLabelResponse, removeLabelResponse.stackTrace(), {"threadId": thread.id});
                        }
                    }
                }
            }

        };

    if e is error {
        log:printError("An error ocurred in reading the emails.", e, e.stackTrace());
        return e;
    }
}

function getLabelIDs(gmail:Client gmailClient, string[] labelsToMatch) returns string[]|error {
    gmail:LabelList labelList = check gmailClient->listLabels("me");
    return from gmail:Label label in labelList.labels
           where labelsToMatch.indexOf(label.name) != ()
           select label.id;
}

function parseEmail(gmail:Message message) returns Email|error {
    string 'from = <string>message.headerFrom;
    string subject = <string>message.headerSubject;
    string body = <string>(check mime:base64Decode(<string>(<gmail:MessageBodyPart>message.emailBodyInText).data));

    return {
        'from: 'from,
        subject: subject,
        body: body
    };
}

function getLead(string 'from, string subject, string emailBody) returns Lead|error {
    Name name = check getName('from);

    return {
        ...name,
        email__c: check getEmailAddress('from),
        phoneNumber__c: "+94771952226",
        company__c: subject,
        designation__c: emailBody
    };
}

function getEmailAddress(string headerFrom) returns string|error {
    int? startIndex = headerFrom.lastIndexOf("<");
    int? endIndex = headerFrom.lastIndexOf(">");

    if (startIndex is () || endIndex is ()) {
        return error("An error ocurred in determining the sender's email address.");
    }

    return headerFrom.substring(startIndex + 1, endIndex);
}

function getName(string headerFrom) returns Name|error {
    int? firstWhitespaceIndex = headerFrom.indexOf(" ");
    int? angleBracketIndex = headerFrom.lastIndexOf(" <");
    if (firstWhitespaceIndex is () || angleBracketIndex is ()) {
        return error("An error occurred in determining the sender's name.");
    }

    int? lastWhitespaceIndex = headerFrom.lastIndexOf(" ", angleBracketIndex-1);
    if (lastWhitespaceIndex is ()) {
        return error("An error occurred in determining the sender's name.");
    }

    string firstName = headerFrom.substring(0, firstWhitespaceIndex);
    string lastName = headerFrom.substring(lastWhitespaceIndex+1, angleBracketIndex);
    return {
        firstName__c: firstName,
        lastName__c: lastName
    };
}