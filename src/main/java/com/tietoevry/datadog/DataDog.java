/*
 * Copyright (C) 2020 Graylog, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.tietoevry.datadog;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.streams.Stream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPOutputStream;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class DataDog implements MessageOutput {

    private final Logger log = LoggerFactory.getLogger(DataDog.class);
    private final CloseableHttpClient httpClient;
    private String apiKey = "";
    private final BlockingQueue<Message> queue;
    private final SendingThread thread;
    private final String url;

    @Inject
    public DataDog(@Assisted Stream stream, @Assisted Configuration conf) {
        String key = conf.getString("apiKey");
        if (!key.equals(""))
            apiKey = key;

        this.url = conf.getString("apiURL");
        int concurrentConnections = conf.getInt("concurrentConnections");

        queue = new ArrayBlockingQueue<>(conf.getInt("packageSize"));

        RequestConfig.Builder requestConfig = RequestConfig.custom();
        requestConfig.setConnectTimeout(6 * 1000);
        requestConfig.setConnectionRequestTimeout(6 * 1000);
        requestConfig.setSocketTimeout(6 * 1000);

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(concurrentConnections);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig.build());
        httpClient = clientBuilder.build();

        thread = new SendingThread(concurrentConnections, queue, this::sendPackage);
        thread.start();
    }

    @Override
    public void stop() {
        thread.stopThread();
        try {
            httpClient.close();
        } catch (IOException e) {
            log.error("Error closing http client", e);
        }
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    private void sendPackage(List<Message> data) {
        JSONArray jsonList = new JSONArray();
        for (Message message : data) {
            JSONObject json = new JSONObject();
            message.getFieldsEntries().forEach(item -> json.put(item.getKey(), item.getValue()));

            String vdom = json.has("vdom") ? json.getString("vdom") : "";
            String lb_partition = json.has("lb_partition") ? json.getString("lb_partition") : "";
            String logType = json.has("log_type") ? json.getString("log_type") : "";

            JSONObject outputJson = new JSONObject();
            outputJson.put("ddsource", "cportal");
            outputJson.put("ddtags", "vdom:" + vdom + ",lb_partition:" + lb_partition + ",log_type:" + logType);
            outputJson.put("hostname", json.has("hostname") ? json.getString("hostname") : "");
            outputJson.put("message", json.toString());
            outputJson.put("service", "cportal");

            jsonList.put(outputJson);
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            gzipOutputStream.write(jsonList.toString().getBytes(StandardCharsets.UTF_8));
            jsonList.clear();
        } catch (IOException e) {
            log.error("Creating GZIP failed", e);
        }
        byte[] gzippedBytes = outputStream.toByteArray();

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setHeader("Content-Encoding", "gzip");
        httpPost.setHeader("DD-API-KEY", apiKey);
        httpPost.setEntity(new ByteArrayEntity(gzippedBytes));

        try(CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode != 202) {
                log.error("Error - wrong response - status code: {}", statusCode);
            }
        } catch (IOException e) {
            log.error("Executing post request failed", e);
        }
    }

    @Override
    public void write(Message message) throws Exception {
        queue.put(message);
        if (queue.remainingCapacity() == 0) {
            thread.notifyThread();
        }
    }

    @Override
    public void write(List<Message> messages) throws Exception {
        for (Message msg : messages) {
            write(msg);
        }
    }

    public interface Factory extends MessageOutput.Factory<DataDog> {
        @Override
        DataDog create(Stream steam, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("HTTP Output Datadog", false, "", "Forwards messages through http");
        }
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();
            configurationRequest.addField(
                    new TextField("apiURL",
                            "API URL",
                            "https://http-intake.logs.datadoghq.eu/api/v2/logs",
                            "API URL",
                            ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(
                    new TextField("apiKey",
                            "DD-API-KEY",
                            "",
                            "API Key",
                            ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(
                    new NumberField("packageSize",
                            "Package size",
                            400,
                            "How many messages should be wrapped in one post request",
                            ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(
                    new NumberField("concurrentConnections",
                            "Number of concurrent connections",
                            3,
                            "Number of concurrent connections",
                            ConfigurationField.Optional.NOT_OPTIONAL));


            return configurationRequest;
        }
    }
}
