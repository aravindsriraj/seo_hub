import schedule
import time
from datetime import datetime
from typing import List, Dict
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ..analysis.engine import CompetitiveAnalysisEngine
from ..core.config import config

class AnalysisPipeline:
    def __init__(self):
        self.engine = CompetitiveAnalysisEngine()
        self.slack_client = WebClient(token=config.SLACK_TOKEN)
        self.alert_thresholds = self._load_alert_thresholds()

    def _load_alert_thresholds(self) -> Dict:
        """Load alert thresholds from configuration."""
        return {
            'ranking_change': 5,  # Positions
            'mention_change': 20,  # Percentage
            'content_updates': 3   # Number of updates
        }

    def run_daily_analysis(self):
        """Run daily analysis and send alerts if needed."""
        try:
            # Run all analyses
            content_analysis = self.engine.analyze_content_updates(1)
            ranking_analysis = self.engine.analyze_ranking_movements(1)
            llm_analysis = self.engine.analyze_llm_mentions(1)
            cross_analysis = self.engine.cross_analyze_metrics(1)
            
            # Check for alert conditions
            alerts = self._check_alert_conditions({
                'content': content_analysis,
                'rankings': ranking_analysis,
                'llm': llm_analysis,
                'cross': cross_analysis
            })
            
            # Generate report
            report = self._generate_report({
                'content': content_analysis,
                'rankings': ranking_analysis,
                'llm': llm_analysis,
                'cross': cross_analysis,
                'alerts': alerts
            })
            
            # Send notifications
            if alerts:
                self._send_alert_notifications(alerts)
            
            # Send daily report
            self._send_daily_report(report)
            
        except Exception as e:
            error_msg = f"Error in daily analysis: {str(e)}"
            self._send_error_notification(error_msg)

    def _check_alert_conditions(self, analyses: Dict) -> List[Dict]:
        """Check for conditions that warrant alerts."""
        alerts = []
        
        # Check ranking changes
        significant_ranking_changes = self._check_ranking_alerts(
            analyses['rankings']['raw_data']
        )
        if significant_ranking_changes:
            alerts.extend(significant_ranking_changes)
        
        # Check LLM mention changes
        mention_alerts = self._check_mention_alerts(
            analyses['llm']['raw_data']
        )
        if mention_alerts:
            alerts.extend(mention_alerts)
        
        # Check competitor content updates
        content_alerts = self._check_content_alerts(
            analyses['content']['raw_data']
        )
        if content_alerts:
            alerts.extend(content_alerts)
        
        return alerts

    def _send_slack_notification(self, message: str, channel: str = "#competitive-intel"):
        """Send notification to Slack."""
        try:
            response = self.slack_client.chat_postMessage(
                channel=channel,
                text=message,
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        }
                    }
                ]
            )
        except SlackApiError as e:
            print(f"Error sending Slack message: {e}")

    def _send_email_notification(self, subject: str, body: str):
        """Send email notification."""
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = config.EMAIL_FROM
        msg['To'] = config.EMAIL_TO
        
        msg.attach(MIMEText(body, 'html'))
        
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.send_message(msg)

    def start(self):
        """Start the automated analysis pipeline."""
        # Schedule daily analysis
        schedule.every().day.at("06:00").do(self.run_daily_analysis)
        
        # Run continuous loop
        while True:
            schedule.run_pending()
            time.sleep(60)