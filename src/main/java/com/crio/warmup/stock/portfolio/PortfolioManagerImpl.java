
package com.crio.warmup.stock.portfolio;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.crio.warmup.stock.dto.AnnualizedReturn;
import com.crio.warmup.stock.dto.Candle;
import com.crio.warmup.stock.dto.PortfolioTrade;
import com.crio.warmup.stock.dto.TiingoCandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.springframework.web.client.RestTemplate;

public class PortfolioManagerImpl implements PortfolioManager {
    // Caution: Do not delete or modify the constructor, or else your build will break!
    // This is absolutely necessary for backward compatibility
    private final RestTemplate restTemplate;

    protected PortfolioManagerImpl(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public List<AnnualizedReturn> calculateAnnualizedReturn(List<PortfolioTrade> portfolioTrades, LocalDate endDate){
        List<AnnualizedReturn> returns = new ArrayList<>();

        for (PortfolioTrade trade : portfolioTrades) {
            try {
                List<Candle> candles = getStockQuote(trade.getSymbol(),
                        trade.getPurchaseDate(), endDate);

                if (candles.isEmpty()) continue;

                double buyPrice = candles.get(0).getOpen();
                double sellPrice = candles.get(candles.size() - 1).getClose();

                double totalReturn = (sellPrice - buyPrice) / buyPrice;
                double totalYears = ChronoUnit.DAYS.between(trade.getPurchaseDate(), endDate) / 365.24;
                double annualizedReturn = Math.pow(1 + totalReturn, 1 / totalYears) - 1;

                returns.add(new AnnualizedReturn(trade.getSymbol(), annualizedReturn, totalReturn));

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return returns.stream().sorted(getComparator())
                .collect(Collectors.toList());
    }


    //TODO: CRIO_TASK_MODULE_REFACTOR
    // 1. Now we want to convert our code into a module, so we will not call it from main anymore.
    //    Copy your code from Module#3 PortfolioManagerApplication#calculateAnnualizedReturn
    //    into #calculateAnnualizedReturn function here and ensure it follows the method signature.
    // 2. Logic to read Json file and convert them into Objects will not be required further as our
    //    clients will take care of it, going forward.

    // Note:
    // Make sure to exercise the tests inside PortfolioManagerTest using command below:
    // ./gradlew test --tests PortfolioManagerTest

    //CHECKSTYLE:OFF




    private Comparator<AnnualizedReturn> getComparator() {
        return Comparator.comparing(AnnualizedReturn::getAnnualizedReturn).reversed();
    }

    //CHECKSTYLE:OFF

    // TODO: CRIO_TASK_MODULE_REFACTOR
    //  Extract the logic to call Tiingo third-party APIs to a separate function.
    //  Remember to fill out the buildUri function and use that.


    public List<Candle> getStockQuote(String symbol, LocalDate from, LocalDate to)
            throws JsonProcessingException {
        String url = buildUri(symbol, from, to);
        TiingoCandle[] candles = restTemplate.getForObject(url, TiingoCandle[].class);
        if (candles == null){
            return new ArrayList<>();
        }
        return Arrays.asList(candles);
    }

    protected String buildUri(String symbol, LocalDate startDate, LocalDate endDate) {
        String apiKey = "Your api key"; // or PortfolioManagerApplication.getToken()
        return "https://api.tiingo.com/tiingo/daily/" + symbol + "/prices?startDate="
                + startDate + "&endDate=" + endDate + "&token=" + apiKey;
    }
}
