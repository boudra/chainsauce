export enum LogLevel {
  none = 0,
  trace,
  debug,
  info,
  warn,
  error,
}

export type LoggerBackend = (level: LogLevel, ...data: unknown[]) => void;

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

  debug(...data: unknown[]) {
    this.log(LogLevel.debug, ...data);
  }

  trace(...data: unknown[]) {
    this.log(LogLevel.trace, ...data);
  }

  info(...data: unknown[]) {
    this.log(LogLevel.info, ...data);
  }

  warn(...data: unknown[]) {
    this.log(LogLevel.warn, ...data);
  }

  error(...data: unknown[]) {
    this.log(LogLevel.error, ...data);
  }

  log(level: LogLevel, ...data: unknown[]) {
    if (level < this.logLevel) {
      return;
    }

    this.backend(level, ...data);
  }
}
