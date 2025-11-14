# Logging Configuration Guide

This project uses a centralized logging system defined in `logging_config.py` to ensure consistent, manageable logging across all scripts.

## Features

- **Rotating File Handlers**: Automatically limits log file sizes (10MB max) and keeps backups (5 files)
- **Consistent Formatting**: All logs use the same timestamp and message format
- **Centralized Configuration**: One place to manage logging behavior
- **Automatic Directory Management**: Creates `logs/` directory automatically
- **Console + File Output**: Logs go to both terminal and file by default

## Quick Start

### Basic Usage

```python
from logging_config import setup_logging

# Create logger for your script
logger = setup_logging('my_script')

# Use it throughout your code
logger.info('Script started')
logger.warning('Something to watch')
logger.error('An error occurred')
logger.debug('Detailed debugging info')
```

### Quick Setup for Simple Scripts

```python
from logging_config import quick_setup

# One-liner setup with default INFO level
logger = quick_setup('my_script')

# Or enable debug mode
logger = quick_setup('my_script', debug=True)
```

### Advanced Configuration

```python
from logging_config import setup_logging
import logging

# Custom log level and file name
logger = setup_logging(
    name='my_script',
    log_level=logging.DEBUG,
    console=True,  # Also log to console
    log_file='custom_name.log'  # Optional custom filename
)
```

## Log Levels

Use appropriate log levels for different messages:

- `logger.debug()` - Detailed diagnostic information (usually disabled in production)
- `logger.info()` - General informational messages about script progress
- `logger.warning()` - Warning messages for potentially problematic situations
- `logger.error()` - Error messages for serious problems
- `logger.critical()` - Critical errors that may cause the script to stop

## Log File Location

All log files are automatically written to: `~/envcan_wave/logs/`

By default, logs are named `{script_name}.log` based on the name you provide to `setup_logging()`.

## Log Rotation

Logs automatically rotate when they reach 10MB:
- Current log: `my_script.log`
- Rotated logs: `my_script.log.1`, `my_script.log.2`, etc.
- Oldest logs are automatically deleted (keeps 5 backups)

## Migrating Existing Scripts

### Before (manual logging setup):

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

### After (centralized logging):

```python
from logging_config import setup_logging

logger = setup_logging('script_name')
```

## Best Practices

1. **Use descriptive logger names**: Name loggers after the script or module
   ```python
   logger = setup_logging('tide_fetch')  # Not 'script' or 'main'
   ```

2. **Log at appropriate levels**: Don't use `info()` for everything
   ```python
   logger.info('Starting data fetch')  # Progress updates
   logger.warning('Station returned no data')  # Potential issues
   logger.error('API request failed')  # Actual errors
   ```

3. **Include context in messages**: Help future debugging
   ```python
   logger.error(f'Failed to fetch data for station {station_id}: {error}')
   ```

4. **Avoid logging sensitive data**: Never log passwords, tokens, or API keys
   ```python
   logger.info('Connected to API')  # Good
   logger.info(f'API token: {token}')  # BAD - don't do this!
   ```

5. **Use exceptions properly**: Log exceptions with traceback
   ```python
   try:
       risky_operation()
   except Exception as e:
       logger.error(f'Operation failed: {e}', exc_info=True)
   ```

## Sarracenia (sr3) Logging

For sr3 processes, logs are configured in `~/.config/sr3/subscribe/*.conf`:

```conf
logDir /home/keelando/envcan_wave/logs
logLevel info
```

This ensures sr3 logs (parser, mqtt, etc.) also go to the centralized `logs/` directory.

## Troubleshooting

**Logs not appearing?**
- Check that `logs/` directory exists (it should be created automatically)
- Verify you're using the correct logger name
- Check file permissions on the logs directory

**Too many debug messages?**
- Change log level: `setup_logging('name', log_level=logging.INFO)`

**Want to disable console output?**
- Use `setup_logging('name', console=False)` for file-only logging

**Need to share a logger across modules?**
```python
from logging_config import get_logger

logger = get_logger('my_script')  # Gets existing logger
```

## Example Scripts

See `parse_marine_forecast.py` for a complete example of the new logging system in action.
