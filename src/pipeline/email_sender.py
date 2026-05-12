"""
Email sender module for SRF Event Monitoring System.

Sends email reports with classification summary, report body, and graph attachments.
Uses SMTP with TLS/SSL support and implements retry logic.
"""

import argparse
import logging
import os
import smtplib
import ssl
import time
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import List, Optional, Union

from ..core.config import get_config, EmailConfig
from ..core.exceptions import EmailError
from ..core.logger import get_logger


logger = get_logger(__name__)


@dataclass
class EmailMessage:
    """Email message data."""
    subject: str
    body_html: Optional[str] = None
    body_plain: Optional[str] = None
    attachments: List[Path] = None
    to: List[str] = None
    cc: List[str] = None
    bcc: List[str] = None

    def __post_init__(self):
        if self.attachments is None:
            self.attachments = []
        if self.to is None:
            self.to = []
        if self.cc is None:
            self.cc = []
        if self.bcc is None:
            self.bcc = []


class EmailSender:
    """Email sender for SRF event reports."""

    def __init__(self, config: Optional[EmailConfig] = None):
        if config is None:
            config = get_config().email

        self.config = config
        self.smtp_server = config.smtp_server
        self.smtp_port = config.smtp_port
        self.sender_email = config.sender_email
        self.sender_password = config.sender_password
        self.default_receivers = config.receiver_emails

        self._validate_config()

        logger.info("Email sender initialized", extra={
            "props": {
                "smtp_server": self.smtp_server,
                "smtp_port": self.smtp_port,
                "sender": self.sender_email,
                "default_receivers": self.default_receivers,
            }
        })

    def _validate_config(self) -> None:
        if not self.smtp_server:
            raise ValueError("SMTP server not configured")
        if not self.sender_email:
            raise ValueError("Sender email not configured")
        if not self.sender_password:
            raise ValueError("Sender password not configured")

    def _create_smtp_connection(self) -> smtplib.SMTP:
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            if not self.sender_password:
                raise EmailError("Sender password not configured")
            server.login(self.sender_email, self.sender_password)
            logger.debug(f"Connected and logged in to {self.smtp_server}:{self.smtp_port}")
            return server
        except (smtplib.SMTPException, ConnectionError, OSError) as e:
            raise EmailError(f"SMTP connection failed: {e}",
                            details={"server": self.smtp_server, "port": self.smtp_port})

    def _build_mime_message(self, email_msg: EmailMessage) -> MIMEMultipart:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = email_msg.subject
        msg['From'] = self.sender_email
        msg['To'] = ', '.join(email_msg.to) if email_msg.to else ''
        if email_msg.cc:
            msg['Cc'] = ', '.join(email_msg.cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Message-ID'] = make_msgid()

        if email_msg.body_plain:
            part1 = MIMEText(email_msg.body_plain, 'plain', 'utf-8')
            msg.attach(part1)
        if email_msg.body_html:
            part2 = MIMEText(email_msg.body_html, 'html', 'utf-8')
            msg.attach(part2)
        if not email_msg.body_plain and not email_msg.body_html:
            part = MIMEText('', 'plain', 'utf-8')
            msg.attach(part)

        for attachment_path in email_msg.attachments:
            if not attachment_path.exists():
                logger.warning(f"Attachment not found: {attachment_path}, skipping")
                continue
            try:
                with open(attachment_path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                filename = attachment_path.name
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
                logger.debug(f"Attached {filename}")
            except (IOError, OSError) as e:
                logger.error(f"Failed to attach {attachment_path}: {e}")

        return msg

    def send(self, email_msg: EmailMessage, max_retries: int = 3, retry_delay: float = 5.0) -> bool:
        recipients = email_msg.to.copy()
        recipients.extend(email_msg.cc)
        recipients.extend(email_msg.bcc)

        if not recipients:
            logger.warning("No recipients specified, using default receivers")
            recipients = self.default_receivers.copy()

        if not recipients:
            raise EmailError("No recipients specified and no default receivers configured")

        msg = self._build_mime_message(email_msg)

        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                server = self._create_smtp_connection()
                try:
                    server.sendmail(self.sender_email, recipients, msg.as_string())
                    logger.info("Email sent successfully", extra={
                        "props": {
                            "subject": email_msg.subject,
                            "recipients": recipients,
                            "attachments": len(email_msg.attachments),
                            "attempt": attempt + 1,
                        }
                    })
                    return True
                finally:
                    try:
                        server.quit()
                    except smtplib.SMTPServerDisconnected:
                        pass
            except (smtplib.SMTPException, EmailError, ConnectionError) as e:
                last_exception = e
                logger.warning(f"Email send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed")

        raise EmailError(
            f"Failed to send email after {max_retries + 1} attempts",
            recipient=recipients[0] if recipients else None,
            details={"last_exception": str(last_exception)}
        )

    def send_report(
        self,
        report_content: str,
        report_format: str = "markdown",
        graph_files: Optional[List[Union[str, Path]]] = None,
        classification_summary: Optional[str] = None,
        to: Optional[List[str]] = None,
        subject_template: Optional[str] = None,
    ) -> bool:
        """Send an email with a generated report."""
        if subject_template is None:
            subject_template = self.config.subject_template

        subject = subject_template
        if classification_summary:
            subject = subject.replace("{case}", classification_summary)
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subject = subject.replace("{timestamp}", timestamp)

        body_html = None
        body_plain = None

        if report_format == "html":
            body_html = report_content
            import re
            body_plain = re.sub(r'<[^>]+>', '', report_content)
        elif report_format == "markdown":
            body_plain = report_content
        else:
            body_plain = report_content

        attachments = []
        if graph_files:
            for gf in graph_files:
                path = Path(gf)
                if path.exists():
                    attachments.append(path)
                else:
                    logger.warning(f"Graph file not found: {gf}")

        email_msg = EmailMessage(
            subject=subject,
            body_html=body_html,
            body_plain=body_plain,
            attachments=attachments,
            to=to if to else self.default_receivers,
        )

        return self.send(email_msg)

    def send_report_from_files(
        self,
        report_file: Union[str, Path],
        graph_files: Optional[List[Union[str, Path]]] = None,
        classification_summary: Optional[str] = None,
        to: Optional[List[str]] = None,
        subject_template: Optional[str] = None,
    ) -> bool:
        """Send an email with report read from a file."""
        report_path = Path(report_file)
        if not report_path.exists():
            raise FileNotFoundError(f"Report file not found: {report_path}")

        ext = report_path.suffix.lower()
        if ext in ['.html', '.htm']:
            format = 'html'
        elif ext in ['.md', '.markdown']:
            format = 'markdown'
        else:
            format = 'plaintext'

        with open(report_path, 'r', encoding='utf-8') as f:
            content = f.read()

        return self.send_report(
            report_content=content, report_format=format,
            graph_files=graph_files, classification_summary=classification_summary,
            to=to, subject_template=subject_template,
        )


def main():
    """Command-line interface for email sender."""
    parser = argparse.ArgumentParser(
        description="SRF Email Sender - Send email reports with attachments"
    )
    parser.add_argument("--report", type=str, required=True,
                        help="Path to report file (markdown/html/plaintext)")
    parser.add_argument("--graphs", type=str, nargs="*", default=[],
                        help="Graph files to attach (jpg/png)")
    parser.add_argument("--to", type=str, nargs="*",
                        help="Recipient email addresses (overrides config)")
    parser.add_argument("--subject", type=str,
                        help="Email subject (overrides template)")
    parser.add_argument("--summary", type=str,
                        help="Classification summary for subject placeholder")
    parser.add_argument("--config", type=str, default="./config/config.yaml",
                        help="Path to configuration file (default: ./config/config.yaml)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    from ..core.config import load_config
    config = load_config(args.config)

    sender = EmailSender(config.email)

    success = sender.send_report_from_files(
        report_file=args.report,
        graph_files=args.graphs if args.graphs else None,
        classification_summary=args.summary,
        to=args.to,
        subject_template=args.subject,
    )

    if success:
        print("Email sent successfully")
        return 0
    else:
        print("Failed to send email")
        return 1


if __name__ == "__main__":
    main()
