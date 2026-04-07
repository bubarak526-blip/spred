using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradingBot.External;
using TradingBot.Models;

/// <summary>
/// Модуль выполнения ордеров (Execution Engine)
/// Отвечает за открытие и закрытие позиций на BingX и Gate с полным набором проверок
/// </summary>
public class ExecutionWorker
{
    private readonly IEnumerable<BingXFuturesOrderBookProvider> _bingXProviders;
    private readonly GateFuturesOrderBookProvider _gateProvider;
    private readonly BingXApiClient _bingXApiClient;
    private readonly GateApiClient _gateApiClient;

    // Отслеживание открытых позиций
    private readonly ConcurrentDictionary<string, OpenPosition?> _openPositions;
    
    // Параметры стратегии
    private readonly decimal _totalOrderSize;
    private readonly decimal _minimumSpreadPercent;
    private readonly decimal _closeThresholdPercent;
    private readonly decimal _feePercent;
    private readonly int _maxOpenSpreads;

    // Параметры выполнения
    private const int MaxRetries = 5;
    private const int RetryDelayMs = 100;
    private const decimal MinimumSpreadToClose = 0.1m;

    public ExecutionWorker(
        IEnumerable<BingXFuturesOrderBookProvider> bingXProviders,
        GateFuturesOrderBookProvider gateProvider,
        BingXApiClient bingXApiClient,
        GateApiClient gateApiClient,
        decimal totalOrderSize,
        decimal minimumSpreadPercent,
        decimal closeThresholdPercent,
        decimal feePercent,
        int maxOpenSpreads)
    {
        _bingXProviders = bingXProviders;
        _gateProvider = gateProvider;
        _bingXApiClient = bingXApiClient;
        _gateApiClient = gateApiClient;

        _totalOrderSize = totalOrderSize;
        _minimumSpreadPercent = minimumSpreadPercent;
        _closeThresholdPercent = closeThresholdPercent;
        _feePercent = feePercent;
        _maxOpenSpreads = maxOpenSpreads;

        _openPositions = new ConcurrentDictionary<string, OpenPosition?>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Попытка открыть спред со всеми проверками
    /// </summary>
    public async Task<ExecutionResult> TryOpenSpreadAsync(
        SpreadAnalysis spread,
        CancellationToken cancellationToken)
    {
        var symbol = spread.Symbol;
        try
        {
            Log.Info($"[EXEC] Проверка возможности открыть спред {symbol}");

            // 1. Проверка лимита открытых позиций
            if (_openPositions.Count >= _maxOpenSpreads)
            {
                Log.Info($"[EXEC] ⏸ Лимит открытых спредов достигнут ({_maxOpenSpreads}). {symbol} перемещён в кандидаты.");
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = $"Лимит открытых спредов ({_maxOpenSpreads}) достигнут"
                };
            }

            // 2. Резервируем слот для позиции (атомарная проверка и резервирование)
            if (!_openPositions.TryAdd(symbol, null))
            {
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = "По этому символу уже открыта позиция"
                };
            }

            // 3. Проверка баланса на обеих биржах
            var bingXBalance = await GetBingXBalanceAsync(cancellationToken);
            var gateBalance = await GetGateBalanceAsync(cancellationToken);

            if (!bingXBalance.IsValid)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = $"Ошибка получения баланса BingX: {bingXBalance.ErrorMessage}"
                };
            }

            if (!gateBalance.IsValid)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = $"Ошибка получения баланса Gate: {gateBalance.ErrorMessage}"
                };
            }

            // 4. Проверка достаточности баланса
            var requiredMargin = Config.PositionSizeUSDT;
            if (bingXBalance.AvailableBalance < requiredMargin)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = $"Недостаточно средств на BingX. Требуется маржа: {requiredMargin:F2} USDT, доступно: {bingXBalance.AvailableBalance:F2} USDT"
                };
            }

            if (gateBalance.AvailableBalance < requiredMargin)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = $"Недостаточно средств на Gate. Требуется маржа: {requiredMargin:F2} USDT, доступно: {gateBalance.AvailableBalance:F2} USDT"
                };
            }

            // 5. Проверка стакана на достаточность и спред
            var liquidityCheck = await ValidateLiquidityAndSpreadAsync(spread, cancellationToken);
            if (!liquidityCheck.IsValid)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                return new ExecutionResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = liquidityCheck.ErrorMessage ?? "Недостаточная ликвидность"
                };
            }

            // 6. Открываем позиции параллельно
            Log.Info($"[EXEC] Открытие спреда {symbol} в направлении {spread.DirectionLabel}");

            var openResult = await OpenPositionsAsync(spread, cancellationToken);

            if (!openResult.Success)
            {
                // Освобождаем зарезервированный слот
                _openPositions.TryRemove(symbol, out _);
                
                if (openResult.ErrorMessage.Contains("margin", StringComparison.OrdinalIgnoreCase) ||
                    openResult.ErrorMessage.Contains("insufficient", StringComparison.OrdinalIgnoreCase))
                {
                    Log.Warn($"[EXEC] Ошибка маржи при открытии {symbol}: {openResult.ErrorMessage}");
                    return new ExecutionResult
                    {
                        Success = false,
                        Symbol = symbol,
                        ErrorMessage = $"Недостаточно маржи: {openResult.ErrorMessage}"
                    };
                }
                
                return openResult;
            }

            // 7. Регистрируем открытую позицию
            var openPosition = new OpenPosition
            {
                Symbol = symbol,
                BingXSymbol = spread.BingXSymbol,
                GateSymbol = spread.GateSymbol,
                Direction = spread.Direction,
                OpenTime = DateTime.UtcNow,
                BuyPrice = spread.BuyPrice,
                SellPrice = spread.SellPrice,
                OrderQty = spread.OrderQtyCoins,
                TotalOrderSize = _totalOrderSize,
                BingXOrderId = openResult.BingXOrderId,
                GateOrderId = openResult.GateOrderId,
                EntrySpread = spread.NetPercent,
                Status = PositionStatus.Open
            };

            _openPositions[symbol] = openPosition;

            Log.Info($"[EXEC] ✓ Спред {symbol} успешно открыт. Вход: {spread.NetPercent:F3}%");

            return new ExecutionResult
            {
                Success = true,
                Symbol = symbol,
                OpenPrice = spread.BuyPrice,
                ClosePrice = spread.SellPrice,
                EstimatedProfit = spread.NetPercent,
                BingXOrderId = openResult.BingXOrderId,
                GateOrderId = openResult.GateOrderId
            };
        }
        catch (Exception ex)
        {
            // Освобождаем зарезервированный слот в случае исключения
            _openPositions.TryRemove(symbol, out _);
            
            Log.Error($"[EXEC] Ошибка при открытии спреда: {ex.Message}", ex);
            return new ExecutionResult
            {
                Success = false,
                Symbol = spread.Symbol,
                ErrorMessage = $"Exception: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Проверка открытой позиции на возможность закрытия
    /// </summary>
    public async Task<CloseResult> TryCloseSpreadAsync(string symbol, CancellationToken cancellationToken)
    {
        try
        {
            if (!_openPositions.TryGetValue(symbol, out var position))
            {
                return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = "Позиция не найдена" };
            }

            if (position == null)
            {
                return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = "Позиция в процессе открытия" };
            }

            // Получаем данные из WebSocket
            var bingXOrderBook = _bingXProviders.Select(p => p.TryGetOrderBook(position.BingXSymbol)).FirstOrDefault(ob => ob != null);
            var gateOrderBook = _gateProvider?.TryGetOrderBook(position.GateSymbol);

            // Проверка наличия данных
            if (bingXOrderBook == null || gateOrderBook == null)
                 return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = "Стаканы недоступны (WS)" };

            // Проверка свежести данных
            var ageB = DateTime.UtcNow - bingXOrderBook.Timestamp;
            var ageG = DateTime.UtcNow - gateOrderBook.Timestamp;
            
            if (ageB.TotalMilliseconds > 2000 || ageG.TotalMilliseconds > 2000)
                 return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = $"Данные устарели (B:{ageB.TotalMilliseconds:0}ms, G:{ageG.TotalMilliseconds:0}ms)" };

            // Рассчитываем цены выхода
            decimal closePrice, buyBackPrice;

            if (position.Direction == SpreadDirection.BingXToGate)
            {
                // Купили на BingX, продали на Gate -> закрываем: продаем на BingX (Asks), покупаем на Gate (Bids)
                closePrice = GetAveragePriceFromBook(bingXOrderBook.Asks, position.OrderQty);
                buyBackPrice = GetAveragePriceFromBook(gateOrderBook.Bids, position.OrderQty);
            }
            else // GateToBingX
            {
                // Купили на Gate, продали на BingX -> закрываем: продаем на Gate (Asks), покупаем на BingX (Bids)
                closePrice = GetAveragePriceFromBook(gateOrderBook.Asks, position.OrderQty);
                buyBackPrice = GetAveragePriceFromBook(bingXOrderBook.Bids, position.OrderQty);
            }

            if (closePrice <= 0 || buyBackPrice <= 0)
            {
                return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = "Неверные цены закрытия" };
            }

            var rawSpread = ((closePrice - buyBackPrice) / buyBackPrice) * 100m;
            var commissionPercent = _feePercent * 2;
            var netClosingSpread = rawSpread - commissionPercent;
            
            var totalPnL = position.EntrySpread + netClosingSpread;
            var closingProfit = Math.Max(totalPnL / 2, netClosingSpread);

            bool shouldClose = false;
            string closeReason = " ";

            if (Math.Abs(netClosingSpread) < 0.1m && netClosingSpread > -0.5m)
            {
                shouldClose = true;
                closeReason = $"Спред сошелся ({netClosingSpread:F3}%)";
            }

            if (totalPnL > _closeThresholdPercent && closingProfit > 0m)
            {
                shouldClose = true;
                closeReason = $"PNL достаточный ({totalPnL:F3}%)";
            }

            if (netClosingSpread < -0.5m)
            {
                shouldClose = true;
                closeReason = $"Спред развалился ({netClosingSpread:F3}%), скат";
            }

            if (!shouldClose)
            {
                return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = $"Условие закрытия не достигнуто. Текущий спред: {netClosingSpread:F3}%" };
            }

            // Закрываем позицию
            Log.Info($"[CLOSE] Закрытие {symbol} - {closeReason}");
            var closePositionResult = await ClosePositionAsync(position, cancellationToken);

            if (!closePositionResult.Success)
            {
                return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = closePositionResult.ErrorMessage };
            }

            _openPositions.TryRemove(symbol, out _);
            Log.Info($"[CLOSE] ✓ Спред {symbol} закрыт. PNL: {totalPnL:F3}%");

            return new CloseResult {
                Success = true,
                Symbol = symbol,
                ClosePrice = closePrice,
                BuyBackPrice = buyBackPrice, 
                NetSpread = netClosingSpread,
                TotalProfit = totalPnL,
                CloseReason = closeReason
            };
        }
        catch (Exception ex)
        {
            Log.Error($"[CLOSE] Ошибка при закрытии спреда {symbol}: {ex.Message}", ex);
            return new CloseResult { Success = false, Symbol = symbol, ErrorMessage = $"Exception: {ex.Message}" };
        }
    }

    /// <summary>
    /// Сканирование всех открытых позиций на возможность закрытия
    /// </summary>
    public async Task<List<CloseResult>> ScanAndClosePositionsAsync(CancellationToken cancellationToken)
    {
        var results = new List<CloseResult>();

        foreach (var symbol in _openPositions.Keys.ToList())
        {
            try
            {
                var closeResult = await TryCloseSpreadAsync(symbol, cancellationToken);
                if (closeResult.Success)
                {
                    results.Add(closeResult);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"[CLOSE] Ошибка при сканировании {symbol}: {ex.Message}", ex);
            }
        }

        return results;
    }

    /// <summary>
    /// Валидация ликвидности и спреда перед открытием
    /// </summary>
    private Task<LiquidityCheckResult> ValidateLiquidityAndSpreadAsync(
        SpreadAnalysis spread,
        CancellationToken cancellationToken)
    {
        try
        {
            // Берём стаканы из кэша провайдеров
            var bingXOB = _bingXProviders.Select(p => p.TryGetOrderBook(spread.BingXSymbol)).FirstOrDefault(ob => ob != null);
            var gateOB = _gateProvider?.TryGetOrderBook(spread.GateSymbol);

            if (bingXOB == null || gateOB == null)
                return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = "Стаканы отсутствуют в кэше" });

            if (bingXOB.Bids.Count == 0 || bingXOB.Asks.Count == 0 || gateOB.Bids.Count == 0 || gateOB.Asks.Count == 0)
                return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = "Стаканы неполные" });

            var ageBingX = DateTime.UtcNow - bingXOB.Timestamp;
            var ageGate = DateTime.UtcNow - gateOB.Timestamp;
            if (ageBingX.TotalMilliseconds > 2000 || ageGate.TotalMilliseconds > 2000)
                return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = $"Стаканы устарели (B:{ageBingX.TotalMilliseconds:0}ms, G:{ageGate.TotalMilliseconds:0}ms)" });

            // Определяем, какие уровни нужны для открытия
            // B->G: покупаем на BingX (спрашиваем Asks), продаём на Gate (спрашиваем Bids)
            var buyLevels = spread.Direction == SpreadDirection.BingXToGate ? bingXOB.Asks : gateOB.Asks;
            var sellLevels = spread.Direction == SpreadDirection.BingXToGate ? gateOB.Bids : bingXOB.Bids;

            decimal requiredQty = spread.OrderQtyCoins;
            if (requiredQty <= 0m) return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = "qty <= 0" });

            // Считаем среднюю цену исполнения
            decimal avgBuyPrice = GetWeightedAvgPrice(buyLevels, requiredQty);
            decimal avgSellPrice = GetWeightedAvgPrice(sellLevels, requiredQty);

            if (avgBuyPrice <= 0m || avgSellPrice <= 0m)
                return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = "Недостаточно ликвидности для расчёта" });

            // Оцениваем проскальзывание
            decimal bestBuy = buyLevels.First().Price;
            decimal bestSell = sellLevels.First().Price;
            
            decimal slippageBuy = Math.Abs(avgBuyPrice - bestBuy) / bestBuy * 100m;
            decimal slippageSell = Math.Abs(avgSellPrice - bestSell) / bestSell * 100m;
            decimal totalSlippage = slippageBuy + slippageSell;

            if (totalSlippage > Config.MaxSlippagePercent)
            {
                return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = $"Проскальзывание {totalSlippage:F2}% > допустимого {Config.MaxSlippagePercent:F2}%" });
            }

            return Task.FromResult(new LiquidityCheckResult { IsValid = true });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new LiquidityCheckResult { IsValid = false, ErrorMessage = ex.Message });
        }
    }

    /// <summary>
    /// Открываем позиции на обеих биржах параллельно
    /// </summary>
    private async Task<ExecutionResult> OpenPositionsAsync(
        SpreadAnalysis spread,
        CancellationToken cancellationToken)
    {
        try
        {
            // Определяем биржи
            var (buyExchange, sellExchange) = GetExchangesFromDirection(spread.Direction);
            var buySymbol = GetSymbolForExchange(buyExchange, spread);
            var sellSymbol = GetSymbolForExchange(sellExchange, spread);

            // Рассчитываем количество
            decimal price = spread.BuyPrice;
            if (price <= 0m)
            {
                return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = "Цена <= 0" };
            }

            decimal rawQty = _totalOrderSize / price;
            decimal qty = Math.Floor(rawQty * 10000m) / 10000m;

            if (qty * price < 5.1m)
                return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = $"Номинал ордера ({qty * price:F2} USDT) < 5.1" };

            Log.Info($"[EXEC] Расчет: {_totalOrderSize} USDT / {price:F4} = {qty:F4} {spread.Symbol}");

            // Открываем позиции
            var buyTask = PlaceOrderAsync(buyExchange, buySymbol, true, qty, cancellationToken);
            var sellTask = PlaceOrderAsync(sellExchange, sellSymbol, false, qty, cancellationToken);
            
            await Task.WhenAll(buyTask, sellTask);

            var buyResult = await buyTask;
            var sellResult = await sellTask;

            if (buyResult.Success && sellResult.Success)
            {
                return new ExecutionResult
                {
                    Success = true,
                    Symbol = spread.Symbol,
                    BingXOrderId = buyExchange == ExchangeType.BingX ? buyResult.OrderId : sellResult.OrderId,
                    GateOrderId = buyExchange == ExchangeType.Gate ? buyResult.OrderId : sellResult.OrderId
                };
            }

            // Обработка частичных сбоев
            if (buyResult.Success && !sellResult.Success)
            {
                Log.Warn($"[OPEN] Частичный сбой {spread.Symbol}: {buyExchange}=OK, {sellExchange}=FAIL. Откатываем...");
                await RollbackOrderAsync(buyExchange, buySymbol, buyResult.OrderId, cancellationToken);
                return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = $"{sellExchange} failed" };
            }

            if (!buyResult.Success && sellResult.Success)
            {
                Log.Warn($"[OPEN] Частичный сбой {spread.Symbol}: {buyExchange}=FAIL, {sellExchange}=OK. Откатываем...");
                await RollbackOrderAsync(sellExchange, sellSymbol, sellResult.OrderId, cancellationToken);
                return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = $"{buyExchange} failed" };
            }

            return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = "Both orders failed" };
        }
        catch (Exception ex)
        {
            Log.Error($"[OPEN] Ошибка при открытии позиций: {ex.Message}", ex);
            return new ExecutionResult { Success = false, Symbol = spread.Symbol, ErrorMessage = ex.Message };
        }
    }

    /// <summary>
    /// Возвращает пару бирж (покупаем на, продаем на) на основании направления спреда
    /// </summary>
    private (ExchangeType BuyExchange, ExchangeType SellExchange) GetExchangesFromDirection(SpreadDirection direction)
    {
        return direction switch
        {
            SpreadDirection.BingXToGate => (ExchangeType.BingX, ExchangeType.Gate),
            SpreadDirection.GateToBingX => (ExchangeType.Gate, ExchangeType.BingX),
            _ => (ExchangeType.BingX, ExchangeType.Gate)
        };
    }

    /// <summary>
    /// Возвращает символ для конкретной биржи
    /// </summary>
    private string GetSymbolForExchange(ExchangeType exchange, SpreadAnalysis spread)
    {
        return exchange switch
        {
            ExchangeType.BingX => spread.BingXSymbol,
            ExchangeType.Gate => spread.GateSymbol,
            _ => spread.BingXSymbol
        };
    }

    /// <summary>
    /// Размещает ордер на указанной бирже
    /// </summary>
    private async Task<OrderResult> PlaceOrderAsync(
        ExchangeType exchange, 
        string symbol, 
        bool isBuy, 
        decimal qty, 
        CancellationToken cancellationToken)
    {
        string orderId = exchange switch
        {
            ExchangeType.BingX => await PlaceBingXMarketOrderAsync(symbol, isBuy, qty, cancellationToken),
            ExchangeType.Gate => await PlaceGateMarketOrderAsync(symbol, isBuy, qty, cancellationToken),
            _ => ""
        };
        
        return string.IsNullOrEmpty(orderId) 
            ? new OrderResult { Success = false, ErrorMessage = "Order placement failed" }
            : new OrderResult { Success = true, OrderId = orderId };
    }

    /// <summary>
    /// Откатывает размещенный ордер
    /// </summary>
    private async Task RollbackOrderAsync(
        ExchangeType exchange, 
        string symbol, 
        string orderId, 
        CancellationToken cancellationToken)
    {
        try
        {
            switch (exchange)
            {
                case ExchangeType.BingX:
                    await _bingXApiClient.CancelOrderAsync(symbol, orderId);
                    break;
                case ExchangeType.Gate:
                    // Gate API does not have explicit cancel order - handle gracefully
                    Log.Debug($"[ROLLBACK] Gate cancel not fully supported yet for {orderId}");
                    break;
            }
        }
        catch (Exception ex)
        {
            Log.Error($"[ROLLBACK] Failed to rollback {exchange} {symbol} {orderId}: {ex.Message}");
        }
    }

    /// <summary>
    /// Закрытие позиции на обеих биржах параллельно
    /// </summary>
    private async Task<ExecutionResult> ClosePositionAsync(
        OpenPosition position,
        CancellationToken cancellationToken)
    {
        const int MaxRetries = 5;
        const int RetryDelayMs = 100;

        try
        {
            if (position.Direction == SpreadDirection.BingXToGate)
            {
                // Закрываем: продаём на BingX (закрываем лонг), покупаем на Gate (закрываем шорт)
                var bingXOrderId = await PlaceBingXMarketOrderAsync(position.BingXSymbol, false, position.OrderQty, cancellationToken);
                var gateOrderId = await PlaceGateMarketOrderAsync(position.GateSymbol, true, position.OrderQty, cancellationToken);

                // Успех на обеих биржах
                if (!string.IsNullOrEmpty(bingXOrderId) && !string.IsNullOrEmpty(gateOrderId))
                {
                    return new ExecutionResult { Success = true, BingXOrderId = bingXOrderId, GateOrderId = gateOrderId };
                }

                // Частичный сбой — пытаемся дооткрыть неудачную сторону
                if (!string.IsNullOrEmpty(bingXOrderId) && string.IsNullOrEmpty(gateOrderId))
                {
                    Log.Warn($"[CLOSE] Частичный сбой {position.Symbol}: BingX=OK, Gate=FAIL. Пытаемся дооткрыть Gate...");
                    for (int i = 0; i < MaxRetries; i++)
                    {
                        await Task.Delay(RetryDelayMs, cancellationToken);
                        gateOrderId = await PlaceGateMarketOrderAsync(position.GateSymbol, true, position.OrderQty, cancellationToken);
                        if (!string.IsNullOrEmpty(gateOrderId))
                        {
                            Log.Info($"[CLOSE] ✓ Gate дооткрыт после {i+1} попытки");
                            return new ExecutionResult { Success = true, BingXOrderId = bingXOrderId, GateOrderId = gateOrderId };
                        }
                    }
                    // Откат: закрываем успешный ордер на BingX
                    Log.Warn($"[CLOSE] ✗ Не удалось дооткрыть Gate после {MaxRetries} попыток. Откатываем позицию на BingX...");
                    await _bingXApiClient.CancelOrderAsync(position.BingXSymbol, bingXOrderId);
                    return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Частичный сбой: откат позиции на BingX" };
                }

                if (string.IsNullOrEmpty(bingXOrderId) && !string.IsNullOrEmpty(gateOrderId))
                {
                    Log.Warn($"[CLOSE] Частичный сбой {position.Symbol}: BingX=FAIL, Gate=OK. Пытаемся дооткрыть BingX...");
                    for (int i = 0; i < MaxRetries; i++)
                    {
                        await Task.Delay(RetryDelayMs, cancellationToken);
                        bingXOrderId = await PlaceBingXMarketOrderAsync(position.BingXSymbol, false, position.OrderQty, cancellationToken);
                        if (!string.IsNullOrEmpty(bingXOrderId))
                        {
                            Log.Info($"[CLOSE] ✓ BingX дооткрыт после {i+1} попытки");
                            return new ExecutionResult { Success = true, BingXOrderId = bingXOrderId, GateOrderId = gateOrderId };
                        }
                    }
                    // Откат: закрываем успешный ордер на Gate
                    Log.Warn($"[CLOSE] ✗ Не удалось дооткрыть BingX после {MaxRetries} попыток. Откатываем ордер на Gate...");
                    // Gate API does not have explicit cancel - handle gracefully
                    Log.Debug($"[CLOSE] Gate cancel not fully supported yet for {gateOrderId}");
                    return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Частичный сбой: откат ордера на Gate" };
                }

                // Провал на обеих биржах
                return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Ошибка при закрытии позиций на обеих биржах" };
            }
            else
            {
                // Обратное направление: продаём на Gate, покупаем на BingX
                var gateOrderId = await PlaceGateMarketOrderAsync(position.GateSymbol, false, position.OrderQty, cancellationToken);
                var bingXOrderId = await PlaceBingXMarketOrderAsync(position.BingXSymbol, true, position.OrderQty, cancellationToken);

                if (!string.IsNullOrEmpty(gateOrderId) && !string.IsNullOrEmpty(bingXOrderId))
                {
                    return new ExecutionResult { Success = true, GateOrderId = gateOrderId, BingXOrderId = bingXOrderId };
                }

                if (!string.IsNullOrEmpty(gateOrderId) && string.IsNullOrEmpty(bingXOrderId))
                {
                    Log.Warn($"[CLOSE] Частичный сбой {position.Symbol}: Gate=OK, BingX=FAIL. Пытаемся дооткрыть BingX...");
                    for (int i = 0; i < MaxRetries; i++)
                    {
                        await Task.Delay(RetryDelayMs, cancellationToken);
                        bingXOrderId = await PlaceBingXMarketOrderAsync(position.BingXSymbol, true, position.OrderQty, cancellationToken);
                        if (!string.IsNullOrEmpty(bingXOrderId))
                        {
                            Log.Info($"[CLOSE] ✓ BingX дооткрыт после {i+1} попытки");
                            return new ExecutionResult { Success = true, GateOrderId = gateOrderId, BingXOrderId = bingXOrderId };
                        }
                    }
                    // Gate API does not have explicit cancel - handle gracefully
                    Log.Debug($"[CLOSE] Gate cancel not fully supported yet for {gateOrderId}");
                    return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Частичный сбой: откат позиции на Gate" };
                }

                if (string.IsNullOrEmpty(gateOrderId) && !string.IsNullOrEmpty(bingXOrderId))
                {
                    Log.Warn($"[CLOSE] Частичный сбой {position.Symbol}: Gate=FAIL, BingX=OK. Пытаемся дооткрыть Gate...");
                    for (int i = 0; i < MaxRetries; i++)
                    {
                        await Task.Delay(RetryDelayMs, cancellationToken);
                        gateOrderId = await PlaceGateMarketOrderAsync(position.GateSymbol, false, position.OrderQty, cancellationToken);
                        if (!string.IsNullOrEmpty(gateOrderId))
                        {
                            Log.Info($"[CLOSE] ✓ Gate дооткрыт после {i+1} попытки");
                            return new ExecutionResult { Success = true, GateOrderId = gateOrderId, BingXOrderId = bingXOrderId };
                        }
                    }
                    await _bingXApiClient.CancelOrderAsync(position.BingXSymbol, bingXOrderId);
                    return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Частичный сбой: откат ордера на BingX" };
                }

                return new ExecutionResult { Success = false, Symbol = position.Symbol, ErrorMessage = "Ошибка при закрытии позиций на обеих биржах" };
            }
        }
        catch (Exception ex)
        {
            Log.Error($"[CLOSE] Ошибка при закрытии позиций: {ex.Message}", ex);
            return new ExecutionResult
            {
                Success = false,
                Symbol = position.Symbol,
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Размещает маркет ордер на BingX
    /// </summary>
    private async Task<string> PlaceBingXMarketOrderAsync(
        string symbol,
        bool isBuy,
        decimal quantity,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await _bingXApiClient.PlaceMarketOrderAsync(symbol, isBuy ? "BUY" : "SELL", quantity);
            
            if (result.Success && !string.IsNullOrEmpty(result.OrderId))
            {
                Log.Info($"[BINGX] {(isBuy ? "BUY" : "SELL")} маркет: {quantity} {symbol} @ Market. OrderId: {result.OrderId}, Status: {result.Status}");
                return result.OrderId;
            }

            Log.Error($"[BINGX] Ошибка размещения маркет-ордера: {result.ErrorMessage}");
            return "";
            
        }
        catch (Exception ex)
        {
            Log.Error($"[BINGX] Ошибка размещения ордера: {ex.Message}", ex);
            return "";
        }
    }

    /// <summary>
    /// Размещает маркет ордер на Gate
    /// </summary>
    private async Task<string> PlaceGateMarketOrderAsync(
        string symbol,
        bool isBuy,
        decimal quantity,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await _gateApiClient.PlaceMarketOrderAsync(symbol, isBuy, quantity);
            
            if (result.Success && !string.IsNullOrEmpty(result.OrderId))
            {
                Log.Info($"[GATE] {(isBuy ? "BUY" : "SELL")} маркет: {quantity} {symbol} @ Market. OrderId: {result.OrderId}, Status: {result.Status}");
                return result.OrderId;
            }

            Log.Error($"[GATE] Ошибка размещения маркет-ордера: {result.ErrorMessage}");
            return "";
            
        }
        catch (Exception ex)
        {
            Log.Error($"[GATE] Ошибка размещения ордера: {ex.Message}", ex);
            return "";
        }
    }

    /// <summary>
    /// Получает баланс на BingX
    /// </summary>
    public async Task<BalanceCheckResult> GetBingXBalanceAsync(CancellationToken cancellationToken)
    {
        try
        {
            var balance = await _bingXApiClient.GetBalanceAsync();
            
            return new BalanceCheckResult
            {
                IsValid = true,
                ExchangeName = "BingX",
                TotalBalance = balance,
                AvailableBalance = balance
            };
        }
        catch (Exception ex)
        {
            Log.Error($"[BALANCE] Ошибка получения баланса BingX: {ex.Message}", ex);
            return new BalanceCheckResult
            {
                IsValid = false,
                ExchangeName = "BingX",
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Получает баланс на Gate
    /// </summary>
    public async Task<BalanceCheckResult> GetGateBalanceAsync(CancellationToken cancellationToken)
    {
        try
        {
            var balance = await _gateApiClient.GetBalanceAsync();
            
            return new BalanceCheckResult
            {
                IsValid = true,
                ExchangeName = "Gate",
                TotalBalance = balance,
                AvailableBalance = balance
            };
        }
        catch (Exception ex)
        {
            Log.Error($"[BALANCE] Ошибка получения баланса Gate: {ex.Message}", ex);
            return new BalanceCheckResult
            {
                IsValid = false,
                ExchangeName = "Gate",
                ErrorMessage = ex.Message
            };
        }
    }

    /// <summary>
    /// Получает информацию об открытых позициях
    /// </summary>
    public IReadOnlyDictionary<string, OpenPosition?> GetOpenPositions()
    {
        return _openPositions.AsReadOnly();
    }

    /// <summary>
    /// Получает статус конкретной позиции
    /// </summary>
    public OpenPosition? GetPositionStatus(string symbol)
    {
        _openPositions.TryGetValue(symbol, out var position);
        return position;
    }

    /// <summary>
    /// Рассчитывает текущий PNL для позиции
    /// </summary>
    public decimal GetCurrentPnL(string symbol)
    {
        if (!_openPositions.TryGetValue(symbol, out var position))
            return 0;

        if (position == null)
            return 0;

        // Получаем текущие стаканы
        var bingXOrderBook = _bingXProviders.Select(p => p.TryGetOrderBook(position.BingXSymbol)).FirstOrDefault(ob => ob != null);
        var gateOrderBook = _gateProvider?.TryGetOrderBook(position.GateSymbol);

        if (bingXOrderBook == null || gateOrderBook == null)
            return 0;

        decimal closePrice, buyBackPrice;

        if (position.Direction == SpreadDirection.BingXToGate)
        {
            closePrice = GetAveragePriceFromBook(bingXOrderBook.Asks, position.OrderQty);
            buyBackPrice = GetAveragePriceFromBook(gateOrderBook.Bids, position.OrderQty);
        }
        else // GateToBingX
        {
            closePrice = GetAveragePriceFromBook(gateOrderBook.Asks, position.OrderQty);
            buyBackPrice = GetAveragePriceFromBook(bingXOrderBook.Bids, position.OrderQty);
        }

        if (closePrice <= 0 || buyBackPrice <= 0)
            return 0;

        // Рассчитываем текущий спред
        var rawSpread = ((closePrice - buyBackPrice) / buyBackPrice) * 100m;
        var commissionPercent = _feePercent * 2;
        var netClosingSpread = rawSpread - commissionPercent;
        
        var totalPnL = position.EntrySpread + netClosingSpread;
        return totalPnL;
    }

    /// <summary>
    /// Проверяет статус ордера
    /// </summary>
    public async Task<OrderStatusInfo> CheckOrderStatusAsync(string symbol, string orderId, ExchangeType exchange)
    {
        try
        {
            if (exchange == ExchangeType.BingX)
            {
                var result = await _bingXApiClient.GetOrderStatusAsync(symbol, orderId);
                if (result == null)
                {
                    return new OrderStatusInfo
                    {
                        Status = "UNKNOWN",
                        IsFilled = false,
                        IsPartiallyFilled = false,
                        ExecutedQty = 0m
                    };
                }
                
                Log.Debug($"[BINGX] Статус ордера {orderId}: {result.Status}, заполнено {result.ExecutedQty}");
                
                return new OrderStatusInfo
                {
                    Status = result.Status,
                    IsFilled = result.Status == "FILLED" || result.Status == "2",
                    IsPartiallyFilled = result.Status == "PARTIALLY_FILLED" || result.Status == "1",
                    ExecutedQty = result.ExecutedQty
                };
            }
            else // Gate
            {
                var result = await _gateApiClient.GetOrderStatusAsync(symbol, orderId);
                if (result == null)
                {
                    return new OrderStatusInfo
                    {
                        Status = "UNKNOWN",
                        IsFilled = false,
                        IsPartiallyFilled = false,
                        ExecutedQty = 0m
                    };
                }
                
                Log.Debug($"[GATE] Статус ордера {orderId}: {result.Status}, заполнено {result.ExecutedQty}");
                
                return new OrderStatusInfo
                {
                    Status = result.Status,
                    IsFilled = result.Status == "finished",
                    IsPartiallyFilled = false,
                    ExecutedQty = result.ExecutedQty
                };
            }
        }
        catch (Exception ex)
        {
            Log.Error($"[{exchange}] Ошибка при проверке статуса {orderId}: {ex.Message}", ex);
            return new OrderStatusInfo
            {
                Status = "ERROR",
                IsFilled = false,
                IsPartiallyFilled = false,
                ExecutedQty = 0m
            };
        }
    }

    // Вспомогательный метод: средневзвешенная цена для заданного объёма
    private decimal GetWeightedAvgPrice(List<OrderBookLevel> levels, decimal targetQty)
    {
        if (levels == null || levels.Count == 0) return 0m;
        
        decimal filledQty = 0m, totalCost = 0m;
        foreach (var level in levels)
        {
            var take = Math.Min(level.Quantity, targetQty - filledQty);
            totalCost += level.Price * take;
            filledQty += take;
            if (filledQty >= targetQty) break;
        }
        return filledQty > 0 ? totalCost / filledQty : 0m;
    }

    /// <summary>
    /// Получает среднюю цену из стакана для заданного размера
    /// </summary>
    private decimal GetAveragePriceFromBook(List<OrderBookLevel> levels, decimal totalSize)
    {
        if (levels == null || levels.Count == 0)
            return 0m;

        decimal totalCost = 0m;
        decimal totalQuantity = 0m;

        foreach (var level in levels)
        {
            if (totalQuantity >= totalSize)
                break;

            decimal quantityToTake = Math.Min(level.Quantity, totalSize - totalQuantity);
            totalCost += level.Price * quantityToTake;
            totalQuantity += quantityToTake;
        }

        return totalQuantity > 0 ? totalCost / totalQuantity : 0m;
    }
}

// ============================================================================
// МОДЕЛИ И ВСПОМОГАТЕЛЬНЫЕ КЛАССЫ
// ============================================================================

/// <summary>
/// Результат выполнения операции открытия спреда
/// </summary>
public class ExecutionResult
{
    public bool Success { get; set; }
    public string Symbol { get; set; } = "";
    
    // При успехе
    public decimal OpenPrice { get; set; }
    public decimal ClosePrice { get; set; }
    public decimal EstimatedProfit { get; set; }
    public string BuyOrderId { get; set; } = "";
    public string SellOrderId { get; set; } = "";
    public ExchangeType BuyExchange { get; set; }
    public ExchangeType SellExchange { get; set; }
    public decimal Quantity { get; set; }
    
    // Legacy для совместимости
    public string GateOrderId { get; set; } = "";
    public string BingXOrderId { get; set; } = "";
    
    // При ошибке
    public string ErrorMessage { get; set; } = "";
}

/// <summary>
/// Результат размещения ордера
/// </summary>
public class OrderResult
{
    public bool Success { get; set; }
    public string OrderId { get; set; } = "";
    public string ErrorMessage { get; set; } = "";
}

/// <summary>
/// Результат операции закрытия спреда
/// </summary>
public class CloseResult
{
    public bool Success { get; set; }
    public string Symbol { get; set; } = "";
    
    // При успехе
    public decimal ClosePrice { get; set; }
    public decimal BuyBackPrice { get; set; }
    public decimal NetSpread { get; set; }
    public decimal TotalProfit { get; set; }
    public string CloseReason { get; set; } = "";
    
    // При ошибке
    public string ErrorMessage { get; set; } = "";
}

/// <summary>
/// Открытая позиция (для отслеживания)
/// </summary>
public class OpenPosition
{
    public string Symbol { get; set; } = "";
    public string BingXSymbol { get; set; } = "";
    public string GateSymbol { get; set; } = "";
    
    public SpreadDirection Direction { get; set; }
    public DateTime OpenTime { get; set; }
    
    public decimal BuyPrice { get; set; }
    public decimal SellPrice { get; set; }
    public decimal OrderQty { get; set; }
    public decimal TotalOrderSize { get; set; }
    
    public string BingXOrderId { get; set; } = "";
    public string GateOrderId { get; set; } = "";
    
    public decimal EntrySpread { get; set; }
    public PositionStatus Status { get; set; }
    
    // Вычисляемые свойства
    public TimeSpan OpenDuration => DateTime.UtcNow - OpenTime;
    public bool IsOldPosition => OpenDuration.TotalSeconds > 300;
}

/// <summary>
/// Статус позиции
/// </summary>
public enum PositionStatus
{
    Open,
    Closing,
    Closed,
    Error
}

/// <summary>
/// Результат проверки баланса
/// </summary>
public class BalanceCheckResult
{
    public bool IsValid { get; set; }
    public string ExchangeName { get; set; } = "";
    public decimal TotalBalance { get; set; }
    public decimal AvailableBalance { get; set; }
    public string ErrorMessage { get; set; } = "";
}

/// <summary>
/// Результат проверки ликвидности
/// </summary>
public class LiquidityCheckResult
{
    public bool IsValid { get; set; }
    public string ErrorMessage { get; set; } = "";
}
