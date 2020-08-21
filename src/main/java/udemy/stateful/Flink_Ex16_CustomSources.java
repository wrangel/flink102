package udemy.stateful;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;

// Runs without params
public class Flink_Ex16_CustomSources {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple3<String, Double, Double>> streamSource = env.addSource(new StockSource("MSFT"));

        ////streamSource.addSink(new ExcelSink(params.get("output")));
        streamSource.print();

        env.execute("Custom Source and Sink");
    }

    // String: company name, Double: Price at current day, Double: Price at previous day
    // Normal source function set up; it runs as long as source data is available
    public static class StockSource implements SourceFunction<Tuple3<String, Double, Double>> {
        public String symbol; // Only stock prices for particular symbol will be fetched
        public int count; // Limit how many times we are pulling from API, s.t. source does not run indefinitely (eliminate in prod env)

        public StockSource(String symbol) {
            this.symbol = symbol;
            this.count = 0;
        }

        // Any data added to the data stream needs to be added to the source context object that comes along with the run method
        public void run(SourceContext<Tuple3<String, Double, Double>> sourceContext) throws Exception {
            while (count < 100) {
                try {
                    // Get stock quote object
                    StockQuote quote = YahooFinance.get(symbol).getQuote();
                    BigDecimal price = quote.getPrice();
                    BigDecimal prevClose = quote.getPreviousClose();
                    sourceContext.collect(Tuple3.of(symbol, price.doubleValue(), prevClose.doubleValue()));
                } catch (Exception ignored) {
                }
            }

        }

        public void cancel() {
        }
    }

}
