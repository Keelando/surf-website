/* -----------------------------
   Centralized Logging Utility
   Provides structured logging with configurable levels
   ----------------------------- */

const LogLevel = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
  NONE: 4
};

class Logger {
  constructor() {
    // Default to INFO in production, DEBUG in development
    this.level = this.getConfiguredLevel();
    this.timers = new Map();
  }

  /**
   * Get log level from localStorage or default
   */
  getConfiguredLevel() {
    try {
      const stored = localStorage.getItem('logLevel');
      if (stored && LogLevel.hasOwnProperty(stored.toUpperCase())) {
        return LogLevel[stored.toUpperCase()];
      }
    } catch (e) {
      // localStorage might not be available
    }

    // Default: INFO for production, DEBUG for local development
    return window.location.hostname === 'localhost' ? LogLevel.DEBUG : LogLevel.INFO;
  }

  /**
   * Set the current log level
   * @param {string} level - One of: DEBUG, INFO, WARN, ERROR, NONE
   */
  setLevel(level) {
    const upperLevel = level.toUpperCase();
    if (LogLevel.hasOwnProperty(upperLevel)) {
      this.level = LogLevel[upperLevel];
      try {
        localStorage.setItem('logLevel', upperLevel);
      } catch (e) {
        // Ignore localStorage errors
      }
      this.info('Logger', `Log level set to ${upperLevel}`);
    } else {
      this.warn('Logger', `Invalid log level: ${level}`);
    }
  }

  /**
   * Format log message with context
   */
  formatMessage(context, message, data) {
    const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
    const prefix = context ? `[${timestamp}] [${context}]` : `[${timestamp}]`;
    return { prefix, message, data };
  }

  /**
   * Log debug messages (verbose information for development)
   * @param {string} context - Component or module name
   * @param {string} message - Log message
   * @param {*} data - Optional data to log
   */
  debug(context, message, data = null) {
    if (this.level <= LogLevel.DEBUG) {
      const formatted = this.formatMessage(context, message, data);
      console.log(`%c${formatted.prefix} ${formatted.message}`, 'color: #9e9e9e', data || '');
    }
  }

  /**
   * Log informational messages
   * @param {string} context - Component or module name
   * @param {string} message - Log message
   * @param {*} data - Optional data to log
   */
  info(context, message, data = null) {
    if (this.level <= LogLevel.INFO) {
      const formatted = this.formatMessage(context, message, data);
      console.log(`%c${formatted.prefix} ${formatted.message}`, 'color: #2196f3', data || '');
    }
  }

  /**
   * Log warning messages
   * @param {string} context - Component or module name
   * @param {string} message - Log message
   * @param {*} data - Optional data to log
   */
  warn(context, message, data = null) {
    if (this.level <= LogLevel.WARN) {
      const formatted = this.formatMessage(context, message, data);
      console.warn(`${formatted.prefix} ${formatted.message}`, data || '');
    }
  }

  /**
   * Log error messages
   * @param {string} context - Component or module name
   * @param {string} message - Log message
   * @param {Error|*} error - Error object or additional data
   */
  error(context, message, error = null) {
    if (this.level <= LogLevel.ERROR) {
      const formatted = this.formatMessage(context, message, error);
      console.error(`${formatted.prefix} ${formatted.message}`, error || '');

      // Log stack trace if available
      if (error && error.stack) {
        console.error('Stack trace:', error.stack);
      }
    }
  }

  /**
   * Start a performance timer
   * @param {string} label - Timer label
   */
  time(label) {
    this.timers.set(label, performance.now());
    this.debug('Performance', `Timer started: ${label}`);
  }

  /**
   * End a performance timer and log duration
   * @param {string} label - Timer label
   */
  timeEnd(label) {
    if (this.timers.has(label)) {
      const start = this.timers.get(label);
      const duration = (performance.now() - start).toFixed(2);
      this.timers.delete(label);
      this.debug('Performance', `${label}: ${duration}ms`);
      return duration;
    } else {
      this.warn('Performance', `Timer not found: ${label}`);
      return null;
    }
  }

  /**
   * Log a group of related messages
   * @param {string} label - Group label
   * @param {Function} callback - Function containing log statements
   */
  group(label, callback) {
    if (this.level <= LogLevel.DEBUG) {
      console.group(label);
      callback();
      console.groupEnd();
    }
  }

  /**
   * Log a table of data (useful for arrays of objects)
   * @param {string} context - Component or module name
   * @param {Array|Object} data - Data to display in table format
   */
  table(context, data) {
    if (this.level <= LogLevel.DEBUG) {
      console.log(`[${context}]`);
      console.table(data);
    }
  }
}

// Create singleton instance
const logger = new Logger();

// Expose logger and setLevel globally
window.logger = logger;
window.setLogLevel = (level) => logger.setLevel(level);

// Log initialization
logger.debug('Logger', 'Logging system initialized', {
  level: Object.keys(LogLevel).find(key => LogLevel[key] === logger.level),
  hostname: window.location.hostname
});
