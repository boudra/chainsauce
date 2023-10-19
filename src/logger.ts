export enum LogLevel {
  none = 0,
  trace,
  debug,
  info,
  warn,
  error,
}

export type LoggerBackend = (
  level: keyof typeof LogLevel,
  message: string,
  data?: Record<string, unknown>
) => void;

export class Logger {
  private logLevel: LogLevel;
  private backend: LoggerBackend;

  constructor(
    logLevel: LogLevel = LogLevel.none,
    backend: LoggerBackend = () => {
      return;
    }
  ) {
    this.logLevel = logLevel;
    this.backend = backend;
  }

  error(message: string, data?: Record<string, unknown>) {
    this.log(LogLevel.error, message, data);
  }

  warn(message: string, data?: Record<string, unknown>) {
    this.log(LogLevel.warn, message, data);
  }

  info(message: string, data?: Record<string, unknown>) {
    this.log(LogLevel.info, message, data);
  }

  debug(message: string, data?: Record<string, unknown>) {
    this.log(LogLevel.debug, message, data);
  }

  trace(message: string, data?: Record<string, unknown>) {
    this.log(LogLevel.trace, message, data);
  }

  log(level: LogLevel, message: string, data = {}) {
    if (level < this.logLevel) {
      return;
    }

    this.backend(LogLevel[level] as keyof typeof LogLevel, message, data);
  }
}
