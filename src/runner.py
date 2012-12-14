import settings
from raven import Client
import time
import os
import email
import raven
import json
import string
from datetime import datetime
import dateutil.parser

class SentryEmailProcessor(object):
    NAME = "sentry"
    EMAIL_SUBJECT_IDENTIFIER = "sentry"
    
    client = Client(settings.SENTRY_DSN)
    
    def process(self, message):
        """Takes a mail message, parses the relevant part, and sends the parsed
        data to Sentry using the Raven client.
        
        The message must be a multipart message with a 'text/plain' part. This 'text/plain'
        part must begin with the correct sentry validation key, followed by json data.
        If the message is not in this format, it will be dropped."""
        
        raised = False
        
        # If message is multipart we only want the text version of the body,
        # this walks the message and gets the body.
        # multipart means dual html and text representations
        if message.get_content_maintype() == 'multipart':
            for part in message.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True)
                    # Check if this is the relevant part of the message
                    if body.startswith(settings.SENTRY_VALIDATION_KEY):
                        json = self.parse_json_message(body[settings.SENTRY_VALIDATION_KEY.__len__()+1:])
                        self.raise_to_sentry(json)
                        raised = True
                        break
        if not raised:
            print "Warning: message is not in the correct format to be processed by SentryEmailProcessor, even though it's subject line indicates that it ought to be. Dropping mail."

    def raise_to_sentry(self, jsondata):
        try:
            event_date = dateutil.parser.parse(jsondata['date'])
        except Exception:
            event_date = datetime.utcnow()
            
        event_data = jsondata['data']
        event_data['server_name'] = jsondata['server_name']

        self.client.capture('Exception', message = jsondata['message'], date = event_date, data = event_data)
        
    def parse_json_message(self, text):
        text = text.replace('\r\n ', '\n')
        text = text.replace('\n', '')
        parsed_data = json.loads(text)
        return parsed_data

class CarbonEmailProcessor(object):
    NAME = "carbon"
    EMAIL_SUBJECT_IDENTIFIER = "j5_parsable"
    
    def process(self, message):
        """Takes a mail message with one or more attachments and stores the attachments
        in a directory specified by settings.WHISPER_STORAGE_PATH
        
        The message must be a multipart message with a 'text/plain' part and one or more attachments.
        The 'text/plain' part must begin with the correct carbon validation key.
        If the message is not in this format, it will be dropped."""
        
        whisper_file_handled = False
        
        # If message is multipart we only want the text version of the body,
        # this walks the message and gets the body.
        # multipart means dual html and text representations
        if message.get_content_maintype() == 'multipart':
            for part in message.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True)
                    # Check if this is the relevant part of the message
                    if body.startswith(settings.CARBON_VALIDATION_KEY):
                        # Re-walk the parts to retrieve the attachments
                        for p in message.walk():
                            if p.get('Content-Disposition') is None or p.get_content_maintype() == 'multipart':
                                continue

                            filename = p.get_filename()
                            file_data = p.get_payload(decode=True)
                            self.handle_whisper_file(filename, file_data)
                            
                            whisper_file_handled = True
                        break
        if not whisper_file_handled:
            print "Warning: message is not in the correct format to be processed by CarbonEmailProcessor, even though it's subject line indicates that it ought to be. Dropping mail."


    def handle_whisper_file(self, filename, file_data):
        path = os.path.join(settings.WHISPER_STORAGE_PATH, self.filename_to_path(filename))
        name = self.get_real_filename(filename)
        full_name = os.path.join(path, name)

        if not os.path.isdir(path):
            os.makedirs(path)

        file = open(full_name, 'w')
        file.write(file_data)
        file.close

    def filename_to_path(self, filename):
        return os.path.join(*(filename.split('.')[:-2] or ['']))

    def get_real_filename(self, filename):
        return '.'.join(filename.split('.')[-2:])

class IMAPReceiver(object):
    if settings.EMAIL_USE_SSL:
        from imaplib import IMAP4_SSL as IMAP4
    else:
        from imaplib import IMAP4
    
    sentry_processor = SentryEmailProcessor()
    carbon_processor = CarbonEmailProcessor()
    processors = [sentry_processor, carbon_processor]

    def start(self):
        
        # Try to connect to mailbox until successful
        while True:
            try:
                connected = False
                connection = self.connect_to_mailbox()
                connected = True
                resp, data = connection.select('INBOX')

                if resp != "OK":
                    raise "The INBOX mailbox does not exist."

                # Continually check for new mails (with sleep time specified in settings.py) 
                while True:
                    
                    for processor in self.processors:
                        print "Checking for new mails for the %s email processor" % processor.NAME
                        
                        typ, msg_ids = connection.search(None, '(SUBJECT "%s" UNSEEN)' % processor.EMAIL_SUBJECT_IDENTIFIER)

                        msg_ids = msg_ids[0].strip()
                        
                        print "msg_ids=", msg_ids

                        if len(msg_ids) > 0:
                            print "Fetching %d mails.." % len(msg_ids.split(' '))
                            msg_ids = string.replace(msg_ids, ' ', ',')
                            typ, msg_data = connection.fetch(msg_ids, '(RFC822)')
                            print "Fetched. Now processing.."

                            for response_part in msg_data:
                                # A response part is either a string '(stuff here)' or a tuple: ('stuff here', 'stuff here')
                                # We only want the tuples, these are actual messages.

                                if isinstance(response_part, tuple):
                                    msg = email.message_from_string(response_part[1])
                                    print "Processing message:", response_part[0]
                                    processor.process(msg)
                            print "Done."
                        else:
                            print "No new messages."

                    time.sleep(settings.RECHECK_DELAY)
            except Exception as e:
                print e
            finally:
                if connected:
                    connection.logout()
                time.sleep(settings.RECHECK_DELAY)

    def connect_to_mailbox(self):
        print 'Connecting to', settings.EMAIL_HOST
        connection = self.IMAP4(settings.EMAIL_HOST)
        print 'Logging in as', settings.EMAIL_USERNAME
        connection.login(settings.EMAIL_USERNAME, settings.EMAIL_PASSWORD)
        print 'Connected'
        return connection

rec = IMAPReceiver()
rec.start()
