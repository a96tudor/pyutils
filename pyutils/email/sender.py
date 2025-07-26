import smtplib
import time
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from socket import error as SocketError
from typing import List, Optional

from pyutils.config.providers import ConfigProvider, SecretValues
from pyutils.email.attachment import Attachment
from pyutils.logging.logger import Logger


class EmailSender:
    def __init__(self, provider: ConfigProvider, logger: Logger):
        self.provider = provider
        self.logger = logger

    @property
    def __email_config_secret(self) -> SecretValues:
        config = self.provider.provide(["email"])
        if config is None:
            raise ValueError("Email configuration not found.")
        return config

    def __add_attachments(
        self, msg: MIMEMultipart, attachments: List[Attachment]
    ) -> None:
        if attachments is None:
            return

        for attachment in attachments:
            part = MIMEApplication(attachment.content, Name=attachment.filename)
            part["Content-Disposition"] = (
                f'attachment; filename="{attachment.filename}"'
            )
            msg.attach(part)

    def __send_configured_message(
        self, unlocked_secret: SecretValues, msg: MIMEMultipart
    ) -> None:
        smtp_server = unlocked_secret.secret["smtp_server"]
        smtp_port = unlocked_secret.secret["smtp_port"]
        smtp_user = unlocked_secret.secret["smtp_user"]
        smtp_password = unlocked_secret.secret["smtp_password"]
        msg["From"] = unlocked_secret.secret["from_email_address"]

        # Send the message
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)

    def __send_with_retries(self, msg: MIMEMultipart) -> None:
        with self.__email_config_secret.unlock() as secret:
            attempts = 0
            number_of_retries = secret.secret.get("number_of_retries", 3)
            retry_delay = secret.secret.get("retry_delay", 5)
            while attempts < number_of_retries:
                try:
                    self.__send_configured_message(secret, msg)
                    self.logger.info("Email sent successfully")
                    break
                except (smtplib.SMTPException, SocketError) as e:
                    attempts += 1
                    self.logger.error(
                        f"Failed to send email: {e}. "
                        f"Retrying ({attempts}/{number_of_retries})..."
                    )
                    time.sleep(retry_delay)

    def send_email(
        self,
        recipient: str,
        subject: str,
        body: str,
        as_html: Optional[bool] = True,
        attachments: Optional[List[Attachment]] = None,
    ) -> None:
        msg = MIMEMultipart()
        msg["To"] = recipient
        msg["Subject"] = subject

        # Attach HTML body
        if as_html:
            msg.attach(MIMEText(body, "html"))
        else:
            msg.attach(MIMEText(body, "plain"))

        self.__add_attachments(msg, attachments)
        self.__send_with_retries()
