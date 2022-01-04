package com.munteanu.diffusion;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.ValueStream.Default;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.json.JSON;
import com.pushtechnology.diffusion.datatype.json.JSONDataType;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String host = "localhost";
        final int port = 8080;
        final String username = "admin";
        final String password = "password";

        final String topic = "my-topic";

        // Connect to Diffusion
        final Session session = Diffusion.sessions()
                                    .principal(username)
                                    .password(password)
                                    .open(String.format("ws://%s:%d",host,port));

        // Create a JSON topic
        final CompletableFuture res1 = session.feature(TopicControl.class).addTopic(topic, TopicType.JSON);
        res1.get();

        // Publish the data
        final JSONDataType jsonDataType = Diffusion.dataTypes().json();
        final JSON value = jsonDataType.fromJsonString("{\"foo\":\"bar\"}");
        final CompletableFuture res2 = session.feature(TopicUpdate.class).set(topic, JSON.class, value);
        res2.get();

        // attach a stream to listen for updates
        session.feature(Topics.class).addStream(topic, JSON.class, new Topics.ValueStream.Default<JSON>() {
            @Override
            public void onValue(String topicPath, TopicSpecification topicSpec, JSON oldValue, JSON newValue) {
                System.out.println("New value for" + topicPath + ": " + newValue.toJsonString());
            }
        });

        // receive updates from the topic
        final CompletableFuture res3 = session.feature(Topics.class).subscribe(topic);
        res3.get();

        session.close();
    }
}
