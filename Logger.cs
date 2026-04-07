using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

/// <summary>
/// Уровни логирования
/// </summary>
public enum LogLevel
{
    Debug = 0,    // Детальная отладочная информация
    Info = 1,     // Информационные сообщения
    Warn = 2,     // Предупреждения
    Error = 3     // Ошибки
}

/// <summary>
/// Расширенный асинхронный логер с поддержкой файлов и уровней логирования
/// </summary>
public sealed class FileLogger : IAsyncDisposable
{
    private readonly Channel<LogEntry> _logChannel;
    private readonly Task _loggingTask;
    private readonly CancellationTokenSource _cts;
    private readonly string _logFilePath;
    private readonly StreamWriter? _fileWriter;
    private volatile LogLevel _fileLogLevel;
    private volatile LogLevel _consoleLogLevel;

    public FileLogger(
        LogLevel fileLogLevel = LogLevel.Debug,
        LogLevel consoleLogLevel = LogLevel.Info,
        string? logDirectory = null)
    {
        // Определяем путь для логов — по умолчанию рядом с корнем проекта
        logDirectory ??= Path.Combine(FindScriptFolder() ?? Directory.GetCurrentDirectory(), "Logs");
        Directory.CreateDirectory(logDirectory);

        string timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        _logFilePath = Path.Combine(logDirectory, $"spreader_{timestamp}.log");

        // Создаем поток для записи в файл
        try
        {
            _fileWriter = new StreamWriter(_logFilePath, true, System.Text.Encoding.UTF8)
            {
                AutoFlush = true
            };
            Console.WriteLine($"Лог пишется в файл: {_logFilePath}");
        }
        catch (Exception ex)
        {
            _fileWriter = null;
            Console.WriteLine($"Не удалось создать лог-файл: {_logFilePath}. Ошибка: {ex.Message}");
        }

        _fileLogLevel = fileLogLevel;
        _consoleLogLevel = consoleLogLevel;

        _logChannel = Channel.CreateUnbounded<LogEntry>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _cts = new CancellationTokenSource();
        _loggingTask = ProcessLogsAsync(_cts.Token);

        // Записываем заголовок логов
        LogToFile($"===== LOG SESSION START: {DateTime.Now:yyyy-MM-dd HH:mm:ss} =====", LogLevel.Info);
    }

    private static string? FindScriptFolder()
    {
        var current = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        if (current == null)
            return null;

        var directory = new DirectoryInfo(current);
        while (directory != null)
        {
            if (directory.EnumerateFiles("*.csproj").Any())
                return directory.FullName;

            directory = directory.Parent;
        }

        return null;
    }

    /// <summary>
    /// Записывает сообщение с уровнем Debug
    /// </summary>
    public void Debug(string message)
    {
        EnqueueLog(message, LogLevel.Debug);
    }

    /// <summary>
    /// Записывает информационное сообщение
    /// </summary>
    public void Info(string message)
    {
        EnqueueLog(message, LogLevel.Info);
    }

    /// <summary>
    /// Записывает предупреждение
    /// </summary>
    public void Warn(string message)
    {
        EnqueueLog(message, LogLevel.Warn);
    }

    /// <summary>
    /// Записывает ошибку с исключением
    /// </summary>
    public void Error(string message, Exception? ex = null)
    {
        var fullMessage = ex is null
            ? message
            : $"{message} | {ex.GetType().Name}: {ex.Message}";
        EnqueueLog(fullMessage, LogLevel.Error);
    }

    private void EnqueueLog(string message, LogLevel level)
    {
        var entry = new LogEntry
        {
            Timestamp = DateTime.UtcNow,
            Level = level,
            Message = message
        };

        _ = _logChannel.Writer.TryWrite(entry);
    }

    private async Task ProcessLogsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var entry in _logChannel.Reader.ReadAllAsync(cancellationToken))
            {
                // Пишем в файл если уровень подходит
                if (entry.Level >= _fileLogLevel)
                {
                    LogToFile(FormatLogEntry(entry), entry.Level);
                }

                // Выводим в консоль если уровень подходит
                if (entry.Level >= _consoleLogLevel)
                {
                    Console.WriteLine(FormatConsoleEntry(entry));
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Нормальное завершение
        }
    }

    private void LogToFile(string message, LogLevel level)
    {
        try
        {
            _fileWriter?.WriteLine(message);
        }
        catch
        {
            // Игнорируем ошибки записи в файл
        }
    }

    public void SetConsoleLogLevel(LogLevel level)
    {
        _consoleLogLevel = level;
    }

    public void SetFileLogLevel(LogLevel level)
    {
        _fileLogLevel = level;
    }

    private string FormatLogEntry(LogEntry entry)
    {
        return $"[{entry.Timestamp:yyyy-MM-dd HH:mm:ss.fff}] [{entry.Level.ToString().ToUpper(),5}] {entry.Message}";
    }

    private string FormatConsoleEntry(LogEntry entry)
    {
        var color = entry.Level switch
        {
            LogLevel.Debug => "[DIM]",
            LogLevel.Info => "[GREEN]",
            LogLevel.Warn => "[YELLOW]",
            LogLevel.Error => "[RED]",
            _ => ""
        };

        var resetColor = entry.Level switch
        {
            LogLevel.Debug or LogLevel.Info or LogLevel.Warn or LogLevel.Error => "[/]",
            _ => ""
        };

        return $"{color}[{entry.Timestamp:HH:mm:ss}] [{entry.Level.ToString().ToUpper(),5}] {entry.Message}{resetColor}";
    }

    public async ValueTask DisposeAsync()
    {
        _logChannel.Writer.TryComplete();
        
        try
        {
            await _loggingTask;
        }
        catch { }

        try
        {
            LogToFile($"===== LOG SESSION END: {DateTime.Now:yyyy-MM-dd HH:mm:ss} =====", LogLevel.Info);
            _fileWriter?.Flush();
            _fileWriter?.Dispose();
        }
        catch { }

        _cts.Cancel();
        _cts.Dispose();
    }

    private struct LogEntry
    {
        public DateTime Timestamp { get; set; }
        public LogLevel Level { get; set; }
        public string Message { get; set; }
    }
}

/// <summary>
/// Глобальный синглтон для логирования
/// </summary>
public static class Log
{
    private static FileLogger? _logger;

    /// <summary>
    /// Инициализирует логер с заданными уровнями
    /// </summary>
    public static void Initialize(
        LogLevel fileLogLevel = LogLevel.Debug,
        LogLevel consoleLogLevel = LogLevel.Info)
    {
        _logger ??= new FileLogger(fileLogLevel, consoleLogLevel);
    }

    public static void SetConsoleLogLevel(LogLevel level)
    {
        _logger?.SetConsoleLogLevel(level);
    }

    public static void SetFileLogLevel(LogLevel level)
    {
        _logger?.SetFileLogLevel(level);
    }

    public static void Debug(string message)
    {
        _logger?.Debug(message);
    }

    public static void Info(string message)
    {
        _logger?.Info(message);
    }

    public static void Warn(string message)
    {
        _logger?.Warn(message);
    }

    public static void Error(string message, Exception? ex = null)
    {
        _logger?.Error(message, ex);
    }

    public static async ValueTask DisposeAsync()
    {
        if (_logger is not null)
        {
            await _logger.DisposeAsync();
            _logger = null;
        }
    }
}
