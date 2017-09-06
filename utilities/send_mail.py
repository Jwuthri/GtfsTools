"""Simple handler for sending the email."""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64


def send_log_mail(sender, receiver, subject, message,
                  file_to_send=None, file_name=None):
    """
    This is a test

    :param sender: the sender as a string
    :param receiver: the receivers as a list or string separate by comma
    :param subject: the object of the mail
    :param message: the message as a string
    :param file_to_send: the path of the log file to send
    :param file_name: the name of the file that will
        append in the mail (could be different from the name of the path)
    """

    msg = MIMEMultipart()
    msg['From'] = str(sender)
    msg['To'] = str(receiver)
    msg['Subject'] = subject
    message = message
    msg.attach(MIMEText(message))
    mailserver = smtplib.SMTP("smtphost.grenoble.xrce.xerox.com", 25)

    if file_name:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file_to_send, "rb").read())
        encode_base64(part)
        part.add_header(
            'Content-Disposition', 'attachment; filename="' + file_name + '"')
        msg.attach(part)

    mailserver.sendmail(sender, receiver, msg.as_string())
    mailserver.quit()
