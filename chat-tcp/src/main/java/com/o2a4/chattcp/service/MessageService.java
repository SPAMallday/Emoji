package com.o2a4.chattcp.service;

import com.o2a4.chattcp.proto.TransferOuterClass.Transfer;
import com.o2a4.chattcp.repository.ChannelIdChannelRepository;
import com.o2a4.chattcp.repository.FilterRepository;
import com.o2a4.chattcp.repository.TrainChannelGroupRepository;
import com.o2a4.chattcp.util.filter.AhoCorasickDoubleArrayTrie;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import com.o2a4.chattcp.util.filter.AhoCorasickDoubleArrayTrie.Hit;

import java.util.LinkedList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final TrainChannelGroupRepository tcgRepo;
    private final ChannelIdChannelRepository cidcRepo;
    private final FilterRepository filterRepo;

    static String uPrefix = "user:";

    // filterMessage가 Mono<String>을 반환하는 경우
    public Mono<String> filterMessage(String content) {
        StringBuilder sb = new StringBuilder();
        String target = content;
        LinkedList<Integer[]> output = new LinkedList<>();

        Mono<String> result = Mono.just(filterRepo.getFilterTrie().customParseText(target))
                .flatMap(filtered -> {
                    // 비속어가 없다면
                    if (filtered == null) { return Mono.just(content); }

                    // 비속어가 있다면
                    List<AhoCorasickDoubleArrayTrie.Hit> res = filtered;
                    // 구간들을 정렬
                    res.sort((o1, o2) -> o1.begin - o2.begin);

                    // 구간들을 병합해서 하나의 리스트로 변환
                    for (AhoCorasickDoubleArrayTrie.Hit interval : res) {
                        if (output.isEmpty() || output.getLast()[1] < interval.begin) {
                            output.add(new Integer[]{interval.begin, interval.end});
                        } else {
                            output.getLast()[1] = Math.max(output.getLast()[1], interval.end);
                        }
                    }

                    // 마스킹처리
                    int ptr = 0;
                    for (Integer[] interval : output) {
                        int start = interval[0];
                        int end = interval[1];
                        // 비속어 시작 전까지 그대로 붙이고 비속어 파트는 *로 대체
                        sb.append(target, ptr, start).append("*");
                        // 비속어 끝(exclusive)을 다음 시작점으로 지정
                        ptr = end;
                    }

                    if (ptr < target.length()) {
                        sb.append(target, ptr, target.length());
                    }

                    return Mono.just(sb.toString());
                });

        return result;
    }

/*
    // filterMessage가 String을 반환할 때
    public String filterMessage(String content) {
        StringBuilder sb = new StringBuilder();
        String target = content;
        LinkedList<Integer[]> output = new LinkedList<>();

        List<AhoCorasickDoubleArrayTrie.Hit<?>> res = filterRepo.getFilterTrie().customParseText(target);

        // 비속어가 있다면
        if (res != null) {
            // 구간들을 정렬
            res.sort((o1, o2) -> o1.begin - o2.begin);

            // 구간들을 병합해서 하나의 리스트로 변환
            for (AhoCorasickDoubleArrayTrie.Hit interval : res) {
                if (output.isEmpty() || output.getLast()[1] < interval.begin) {
                    output.add(new Integer[]{interval.begin, interval.end});
                } else {
                    output.getLast()[1] = Math.max(output.getLast()[1], interval.end);
                }
            }

            // 마스킹처리
            int ptr = 0;
            for (Integer[] interval : output) {
                int start = interval[0];
                int end = interval[1];

                // 비속어 시작 전까지 그대로 붙이고 비속어 파트는 *로 대체
                sb.append(target, ptr, start).append("*");
                // 비속어 끝(exclusive)을 다음 시작점으로 지정
                ptr = end;
            }

            if (ptr < target.length()) {
                sb.append(target, ptr, target.length());
            }
        }

        return sb.toString();
    }
*/

    public void sendMessage(Transfer trans, String msg) {
        String userId = trans.getUserId();

        Transfer send = Transfer.newBuilder(trans).setContent(msg).build();

        redisTemplate.opsForHash().get(uPrefix + userId, "channelGroup")
                .subscribe(cg -> {
                    // 열차 채팅방에 사용자의 메세지 전달
                    log.info("TRANSFER MESSAGE TO ROOM");
                    tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(send);
                });
    }

    public void sendMessageToOne(Transfer trans, String userId) {
        redisTemplate.opsForHash().get(uPrefix + userId, "channel")
                .subscribe(c -> {
                    // 유저 한 명에게 메시지 전송
                    log.info("SEND MESSAGE TO USER");
                    cidcRepo.getChannelIdChannelMap().get(c).writeAndFlush(trans);
                });
    }

    public void sendMessageToRoom(Transfer trans, String type, String target) {
        // 채팅방에 메시지 전송
        if (type.equals("userId")) {
            redisTemplate.opsForHash().get(uPrefix + target, "channelGroup")
                    .subscribe(cg -> {
                        log.info("SEND MESSAGE TO ROOM");
                        tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(trans);
                    });
        } else {
            log.info("SEND MESSAGE TO ROOM");
            tcgRepo.getTrainChannelGroupMap().get(target).writeAndFlush(trans);
        }
    }

    /*
    private static Transfer serverTrans(String type, String content) {
        Transfer.Builder builder = Transfer.newBuilder();

        builder.setType(type);
        builder.setContent(content);

        // 서버로 고정
        builder.setUserId("SERVER");
        builder.setNickName("SERVER");
        builder.setSendAt(LocalDateTime.now().toString());

        return builder.build();
    }*/
}
