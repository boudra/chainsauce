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
  data: unknown,
  message?: string
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

  error(data: unknown, message?: string) {
    this.log(LogLevel.error, data, message);
  }

  warn(data: unknown, message?: string) {
    this.log(LogLevel.warn, data, message);
  }

  info(data: unknown, message?: string) {
    this.log(LogLevel.info, data, message);
  }

  trace(data: unknown, message?: string) {
    this.log(LogLevel.trace, data, message);
  }

  debug(data: unknown, message?: string) {
    this.log(LogLevel.debug, data, message);
  }

  log(level: LogLevel, data: unknown, message?: string) {
    if (level < this.logLevel) {
      return;
    }

    this.backend(LogLevel[level] as keyof typeof LogLevel, data, message);
  }
}
