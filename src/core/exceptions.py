"""
Custom exceptions for SRF Event Monitoring System.
"""

from typing import Any, Optional


class SRFError(Exception):
    """Base exception for all SRF monitoring errors."""
    
    def __init__(self, message: str = "", details: Optional[Any] = None):
        """
        Initialize SRF error.
        
        Args:
            message: Error message.
            details: Additional error details (e.g., dict with context).
        """
        self.message = message
        self.details = details
        super().__init__(message)
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} (details: {self.details})"
        return self.message


class ConfigurationError(SRFError):
    """Configuration-related errors."""
    
    def __init__(self, message: str = "", config_path: Optional[str] = None, details: Optional[Any] = None):
        """
        Initialize configuration error.
        
        Args:
            message: Error message.
            config_path: Path to configuration file (if applicable).
            details: Additional error details.
        """
        self.config_path = config_path
        full_message = message
        if config_path:
            full_message = f"{message} [config: {config_path}]"
        
        super().__init__(full_message, details)


class FileProcessingError(SRFError):
    """File processing errors (I/O, permissions, etc.)."""
    
    def __init__(self, message: str = "", file_path: Optional[str] = None, details: Optional[Any] = None):
        """
        Initialize file processing error.
        
        Args:
            message: Error message.
            file_path: Path to problematic file (if applicable).
            details: Additional error details.
        """
        self.file_path = file_path
        full_message = message
        if file_path:
            full_message = f"{message} [file: {file_path}]"
        
        super().__init__(full_message, details)


class ValidationError(SRFError):
    """Data validation errors."""
    
    def __init__(self, message: str = "", field: Optional[str] = None, value: Optional[Any] = None, details: Optional[Any] = None):
        """
        Initialize validation error.
        
        Args:
            message: Error message.
            field: Field name that failed validation.
            value: Invalid value (if applicable).
            details: Additional error details.
        """
        self.field = field
        self.value = value
        full_message = message
        if field:
            full_message = f"{message} [field: {field}]"
            if value is not None:
                full_message = f"{full_message}, value: {value}"
        
        super().__init__(full_message, details)


class ProcessingError(SRFError):
    """General processing errors (algorithmic, business logic)."""
    
    def __init__(self, message: str = "", step: Optional[str] = None, data: Optional[Any] = None, details: Optional[Any] = None):
        """
        Initialize processing error.
        
        Args:
            message: Error message.
            step: Processing step where error occurred.
            data: Relevant data (if applicable).
            details: Additional error details.
        """
        self.step = step
        self.data = data
        full_message = message
        if step:
            full_message = f"{message} [step: {step}]"
        
        super().__init__(full_message, details)


class EmailError(SRFError):
    """Email sending errors."""
    
    def __init__(self, message: str = "", recipient: Optional[str] = None, details: Optional[Any] = None):
        """
        Initialize email error.
        
        Args:
            message: Error message.
            recipient: Recipient email address (if applicable).
            details: Additional error details.
        """
        self.recipient = recipient
        full_message = message
        if recipient:
            full_message = f"{message} [recipient: {recipient}]"
        
        super().__init__(full_message, details)


class MonitoringError(SRFError):
    """System monitoring errors."""
    
    def __init__(self, message: str = "", component: Optional[str] = None, details: Optional[Any] = None):
        """
        Initialize monitoring error.
        
        Args:
            message: Error message.
            component: Monitoring component (e.g., "system_tray", "notifications").
            details: Additional error details.
        """
        self.component = component
        full_message = message
        if component:
            full_message = f"{message} [component: {component}]"
        
        super().__init__(full_message, details)