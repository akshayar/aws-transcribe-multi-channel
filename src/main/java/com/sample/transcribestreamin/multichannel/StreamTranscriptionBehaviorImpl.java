// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.sample.transcribestreamin.multichannel;

import software.amazon.awssdk.services.transcribestreaming.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// snippet-start:[transcribe.java-streaming-client-behavior-imp]
public class StreamTranscriptionBehaviorImpl implements StreamTranscriptionBehavior {
    StringBuffer finalResult = new StringBuffer();
    @Override
    public void onError(Throwable e) {
        System.out.println("=== Failure encountered ===");
        e.printStackTrace();
    }

    @Override
    public void onStream(TranscriptResultStream e){
        onStreamWithChannel(e);
    }
    private void onStreamWithChannel(TranscriptResultStream e){
        List<Result> results = ((TranscriptEvent) e).transcript().results();
        Map<String,List<Alternative>> partialTranscript = results.stream().filter(Result::isPartial)
                .collect(Collectors.toMap(Result::channelId,Result::alternatives));

        Map<String,List<Alternative>>  nonPartialTranscript = results.stream().filter(r -> !r.isPartial())
                .collect(Collectors.toMap(Result::channelId,Result::alternatives));

        nonPartialTranscript.forEach((channel,alternatives)->{
            alternatives.stream().forEach(a -> {
                if(!a.transcript().isEmpty()){
//                    System.out.println();
//                    System.out.println("<<"+channel+">>  : "+ a.transcript());
                    System.out.println("<<"+channel+">>  : "+ getSpeakerLabels(a.items().stream()));
                    finalResult.append(" ").append(a.transcript());
                }
            });
        });
    }

    public String getSpeakerLabels(Stream<Item> stream){
        return ""+stream.collect(Collectors.groupingBy(
                this::speaker,
                Collectors.mapping(
                        Item::content,
                        Collectors.joining(" ", " ", " ")
                )
        ));
    }
    public  String speaker(Item item){
        return "speaker_"+item.speaker();
    }


    @Override
    public void onResponse(StartStreamTranscriptionResponse r) {
        System.out.println(String.format("=== Received initial response. Request Id: %s ===", r.requestId()));
    }

    @Override
    public void onComplete() {
        System.out.println(finalResult);
        System.out.println("=== All records streamed successfully ===");
    }
}
// snippet-end:[transcribe.java-streaming-client-behavior-imp]
