package com.o2a4.chattcp.service;

import com.o2a4.chattcp.proto.TransferOuterClass;
import com.o2a4.chattcp.repository.ChannelIdChannelRepository;
import com.o2a4.chattcp.repository.TrainChannelGroupRepository;
import io.netty.channel.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import com.o2a4.chattcp.proto.TransferOuterClass.Transfer;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
@Service
@RequiredArgsConstructor
public class RoomService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final TrainChannelGroupRepository tcgRepo;
    private final ChannelIdChannelRepository cidcRepo;
    private final WebClient webClient;

    static String uPrefix = "user:";
    static String tPrefix = "train:";
    static String sPrefix = "server:";

    public void roomIn(Channel channel, Transfer trans) {
        String userId = trans.getUserId();

        redisTemplate.opsForHash().get("user:" + userId, "channelGroup")
            .flatMap(
                cg -> {
                    String channelId = channel.id().asShortText();

                    // 열차 채팅방에 채널 추가
                    tcgRepo.getTrainChannelGroupMap().get(cg).add(channel);
                    // 채널Id 채널 맵에 추가
                    cidcRepo.getChannelIdChannelMap().put(channelId, channel);

                    return redisTemplate.opsForHash().put(uPrefix+userId, "channel", channelId).doOnSubscribe(
                        i -> {
                            log.info("ROOM IN MESSAGE SENDING");
                            tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(trans);
                        }
                    );
                })
                .subscribe();
    }

    public void roomOut(Channel channel, Transfer trans) {
        String userId = trans.getUserId();

        redisTemplate.opsForHash().get("user:" + userId, "channelGroup")
                .flatMap(
                        cg -> {
                            String channelId = channel.id().asShortText();

                            // 열차 채팅방에서 채널 제거 (close하면 알아서 group에서 제거됨)
                            channel.close();
                            // 채널Id 채널 맵에서 제거
                            cidcRepo.getChannelIdChannelMap().remove(channelId);

                            return redisTemplate.opsForHash().put(uPrefix+userId, "channel", channelId).doOnSubscribe(
                                    i -> {
                                        log.info("ROOM IN MESSAGE SENDING");
                                        tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(trans);
                                    }
                            );
                        })
                .subscribe();
    }

    public String seatStart(String userId) {
        // TODO 자리양도 시작
        // userId : 자리양도 시작한 사용자 아이디
        String winnerId = webClient.get().uri("/seat/"+userId).retrieve().bodyToMono(String.class).block();
        log.info("자리양도 당첨자 id :" + winnerId );
        return winnerId;

    }

    public void seatEnd(Transfer trans) {
        // TODO 자리양도 끝
    }

    public void villainOn(Transfer trans) {
        redisTemplate.opsForHash().get("user:" + trans.getUserId(), "channelGroup")
            .subscribe(cg -> {
                redisTemplate.opsForHash().increment("train:" + (String) cg, "villain", 1).subscribe();
                tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(trans);
            });
    }

    public void villainOff(TransferOuterClass.Transfer trans) {
        redisTemplate.opsForHash().get("user:" + trans.getUserId(), "channelGroup")
            .subscribe(cg -> {
                redisTemplate.opsForHash().get("train:" + (String) cg, "villain")
                        .flatMap(num ->
                                redisTemplate.opsForHash().put("train:" + (String) cg, "villain", Integer.valueOf((String) num)-1))
                        .subscribe();

                tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(trans);
            });
    }
}
