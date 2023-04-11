package multithread;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class GetCountry {
    private BlockingQueue<String> queue;
    private BlockingQueue<String> writeQueue;
    private static final String URL = "https://www.omdbapi.com/?apikey=4ec90a9&i=";
    private static final String OUTPUT_FILE = "/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/full_movies/title.country2.tsv";
    private static final String INPUT_FILE = "/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/full_movies/movies.tsv";
    private static final String SET_FILE = "/Users/qianshameng/Documents/Academic/NEU/Course/6240_big_data/project/data/imdb/full_movies/set2.txt";
    private static final int NUM_OF_THREADS = 200;
    private static final String END_SIGNAL = "END";
    private CountDownLatch countDownLatchApiCall;
    private CountDownLatch countDownLatchWriter;
    private final Set<String> countries;

    public GetCountry() {
        this.queue = new LinkedBlockingQueue<>();
        this.writeQueue = new LinkedBlockingQueue<>();
        countDownLatchApiCall = new CountDownLatch(NUM_OF_THREADS);
        countDownLatchWriter = new CountDownLatch(1);
        countries = new HashSet<>();
    }

    public void run() throws IOException, InterruptedException {
        // Single thread to generate TitleId
        Thread generatingThread = new Thread(() -> {
            TsvParserSettings parserSettings = new TsvParserSettings();
            TsvParser parser = new TsvParser(parserSettings);

            try (BufferedReader reader = new BufferedReader(new FileReader(INPUT_FILE))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = parser.parseLine(line);
                    String titleId = fields[0];
                    queue.put(titleId);
                }

                for (int i = 0; i < NUM_OF_THREADS; i++) {
                    queue.put(END_SIGNAL);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        generatingThread.start();

        // Multithreads that do api call to get country
        for (int i = 0; i < NUM_OF_THREADS; i++) {
            Thread newThread = new Thread(() -> {
                try {
                    while(true) {
                        String titleId = queue.take();
                        if (titleId.equals(END_SIGNAL)) {
                            break;
                        }

                        String country = getCountry(titleId);
                        writeQueue.put(titleId + "\t" + country);
                        System.out.println(titleId + "\t" + country);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    countDownLatchApiCall.countDown();
                }
            });

            newThread.start();
        }

        // Single thread to write into a file
        BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE, true));
        Thread writeThread = new Thread(() -> {
            try {
                while (true) {
                    String line = writeQueue.take();
                    if (line.equals(END_SIGNAL)) {
                        break;
                    }
                    line = line.replaceAll("\"", "");
                    countries.add((line.split("\t"))[1]);
                    writer.write(line + "\n");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatchWriter.countDown();
            }
        });

        writeThread.start();
        countDownLatchApiCall.await();
        writeQueue.put(END_SIGNAL);
        countDownLatchWriter.await();
        writer.close();

        try(BufferedWriter writer2 = new BufferedWriter(new FileWriter(SET_FILE, true))) {
            for (String c: countries) {
                writer2.write(c + "\n");
            }
        }
    }

    private String getCountry(String titleId) throws IOException {
        HttpRequestRetryHandler retryHandler = (exception, executionCount, context) -> {
            if (executionCount >= 5) {
                // Do not retry if over max retry count
                return false;
            }

            return false;
        };

        CloseableHttpClient client = HttpClientBuilder.create().setRetryHandler(retryHandler).build();
        HttpGet request = new HttpGet(URL + titleId);

        String responseBody = client.execute(request, response -> {
            return EntityUtils.toString(response.getEntity());
        });

        JsonObject jsonObject = JsonParser.parseString(responseBody).getAsJsonObject();
        String country = jsonObject.get("Country") == null ? "" : jsonObject.get("Country").toString();
        if (country.isEmpty() || country.equals("N/A")) {
            country = "\\N";
        }
        return country;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        GetCountry getCountry = new GetCountry();
        getCountry.run();
    }
}
