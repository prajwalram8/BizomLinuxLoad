import base64
import logging
from datetime import datetime as dt
from sendgrid import SendGridAPIClient
from .datetime_utilities import dt_to_string
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)

## Logging
# create logger with '__main__'
logger = logging.getLogger('__main__.' + __name__)

default_html_string = '''
    <!DOCTYPE html>
    <html lang="en" xmlns="https://www.w3.org/1999/xhtml" xmlns:o="urn:schemas-microsoft-com:office:office">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <meta name="x-apple-disable-message-reformatting">
        <title></title>
        <!--[if mso]>
        <style>
            table {border-collapse:collapse;border-spacing:0;border:none;margin:0;}
            div, td {padding:0;}
            div {margin:0 !important;}
        </style>
        <noscript>
            <xml>
                <o:OfficeDocumentSettings>
                <o:PixelsPerInch>96</o:PixelsPerInch>
                </o:OfficeDocumentSettings>
            </xml>
        </noscript>
        <![endif]-->
        <style>
            table, td, div, h1, p {
                font-family: Arial, sans-serif;
            }
        </style>
    </head>
    <body style="margin:0;padding:0;word-spacing:normal;background-color:#939297;">
        <div role="article" aria-roledescription="email" lang="en" style="text-size-adjust:100%;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;background-color:#939297;">
            <h1>Ingestion status report attached</h1>
            <a href="https://app.snowflake.com"> Please visit the link for more information </a>
        </div>
    </body>
    </html>
    '''


# Loading the html string for the email template
try:
    with open('shared_modules/mail_template.html', 'r') as f:
        html_string = f.read()
    logger.info("HTML template found. Using it for the body of the mail")
except FileNotFoundError:
    logger.info("HTML template file not found. Setting html string to default template")
    html_string = default_html_string



def encode_file(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()
        f.close()
    encoded_file = base64.b64encode(data).decode()
    return encoded_file


def send_mail(FILE_PATHS, DEL_LIST, FILE_NAMES, API_QY, NAME,FILE_TYPES=['application/csv', 'plain/text']):
    
    message = Mail(
        from_email='gagalertslistener@gmail.com',
        to_emails=DEL_LIST,
        subject=f'Data Ingestion Status | {NAME} | {dt_to_string(dt.now())}',
        html_content=html_string
        )
        
    for each in range(len(FILE_PATHS)):
        encoded_file = encode_file(file_path=FILE_PATHS[each])

        attachedFile = Attachment(
            FileContent(encoded_file),
            FileName(FILE_NAMES[each]),
            FileType(FILE_TYPES[each]),
            Disposition('attachment')
            )

        message.attachment = attachedFile

    sg = SendGridAPIClient(api_key=API_QY)
    response = sg.send(message)

    logger.debug(f"Mail reponse details for: {response.status_code}, {response.body}, {response.headers}")
    logger.info("Mail Successfuly triggered")

    return None

def send_mail_update(DEL_LIST, API_QY, NAME):

    # Loading the html string for the email template
    try:
        with open('shared_modules/mail_template_empty.html', 'r') as f:
            html_string = f.read()
        logger.info("HTML template found. Using it for the body of the mail")
    except FileNotFoundError:
        logger.info("HTML template file not found. Setting html string to default template")
        html_string = default_html_string

    message = Mail(
        from_email='gagalertslistener@gmail.com',
        to_emails=DEL_LIST,
        subject=f'Data Ingestion Status | {NAME} |{dt_to_string(dt.now())}',
        html_content=html_string
    )

    sg = SendGridAPIClient(api_key=API_QY)
    response = sg.send(message)

    logger.debug(f"Mail reponse details for: {response.status_code}, {response.body}, {response.headers}")
    logger.info("Mail Successfuly triggered")

    return None

if __name__ == "__main__":
    send_mail_update("kdb081293@gmail.com", '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')