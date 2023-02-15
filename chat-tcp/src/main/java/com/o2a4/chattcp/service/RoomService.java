package com.o2a4.chattcp.service;

import com.o2a4.chattcp.model.Seats;
import com.o2a4.chattcp.proto.TransferOuterClass.Transfer;
import com.o2a4.chattcp.repository.ChannelIdChannelRepository;
import com.o2a4.chattcp.repository.TrainChannelGroupRepository;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RoomService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final TrainChannelGroupRepository tcgRepo;
    private final ChannelIdChannelRepository cidcRepo;
    private final MessageService messageService;


    static String uPrefix = "user:";
    static String tPrefix = "train:";
    static String sPrefix = "server:";
    static String cPrefix = "channel:";
    static String vPrefix = "villain:";

    public void roomIn(Channel channel, Transfer trans) {
        String userId = trans.getUserId();

        Mono errMsg = Mono.just(userId)
                .doOnSubscribe(a -> {
                    Transfer send = Transfer.newBuilder()
                            .setContent("채팅방 입장 실패")
                            .setType("error")
                            .setUserId("SERVER")
                            .setSendAt(LocalDateTime.now().toString())
                            .build();

                    channel.writeAndFlush(send);
                });

        redisTemplate.opsForHash().get(uPrefix + userId, "channelGroup")
                .flatMap(
                        cg -> {
                            String channelId = channel.id().asShortText();

                            ChannelGroup channelGroup = tcgRepo.getTrainChannelGroupMap().get(cg);

                            // 이 채팅서버에 할당된 열차가 아님
                            if (channelGroup == null) {
                                return Mono.zip(errMsg.subscribeOn(Schedulers.parallel()),
                                        redisTemplate.opsForHash().delete(uPrefix + userId).subscribeOn(Schedulers.parallel()));
                            }

                            // 열차 채팅방에 채널 추가
                            channelGroup.add(channel);
                            // 채널Id 채널 맵에 추가
                            cidcRepo.getChannelIdChannelMap().put(channelId, channel);
                            // 채널Id 유저id Redis에 저장
                            return redisTemplate.opsForValue().set(cPrefix + channelId, userId)
                                    .flatMap(i ->
                                            redisTemplate.expire(cPrefix + channelId, Duration.ofHours(3))
                                    ).flatMap(i ->
                                            redisTemplate.opsForHash().get(tPrefix + cg, "villain")
                                    ).flatMap(v -> {
                                        // Content에 채팅 방 인원과 빌런 수 (a, b) 전달
                                        Transfer send = Transfer.newBuilder(trans).setContent(channelGroup.size() + "," + v.toString()).build();

                                        Map<String, String> aMap = new HashMap<>();
                                        aMap.put("channel", channelId);
                                        aMap.put("nickName", trans.getNickName());

                                        return redisTemplate.opsForHash().putAll(uPrefix + userId, aMap)
                                                .doOnSubscribe(a -> {
                                                            log.info("ROOM IN MESSAGE SENDING");
                                                            tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(send);
                                                        }
                                                );
                                    });
                        })
                .subscribe();
    }

    public void roomOut(Channel channel) {
        channel.close();
    }

    public Mono<String> seatStart(String userId) {
        Mono start = redisTemplate.opsForHash().get(uPrefix + userId, "token")
                .flatMap(token -> {
                            WebClient webClient = WebClient.create();

                            return webClient.get().uri("http://k7a6022.p.ssafy.io/seat/" + userId)
                                    .header("Authorization", "Bearer " + token)
                                    .retrieve().bodyToMono(String.class);
                        }
                );

        return start;
    }

    public void seatEnd(Seats seat) {
        log.info("자리 양도 종료 호출");
        String userId = seat.getUserId();
        String winnerId = seat.getWinnerId();
        String time = LocalDateTime.now().toString();

        // filterMessage가 Mono<String>을 반환하는 경우
        Mono.defer(() -> messageService.filterMessage(seat.getContent()))    // 위치 정보에서 비속어 필터링
                .flatMap(place -> {
                    Transfer.Builder builder = Transfer.newBuilder()
                            .setType("seat-win")
                            .setContent(place)
                            .setUserId(userId)
                            .setSendAt(time);

                    Transfer.Builder b2 = Transfer.newBuilder(builder.build())
                            .setType("seat-end")
                            .setContent("");

                    return redisTemplate.opsForHash().get(uPrefix + winnerId, "channel")
                            .doOnNext(c -> {
                                // 당첨자 메시지 전송
                                log.info("SEND MESSAGE TO USER");
                                cidcRepo.getChannelIdChannelMap().get(c).writeAndFlush(builder.build());
                            }).flatMap(c ->
                                    redisTemplate.opsForHash().get(uPrefix + userId, "channelGroup")
                                            .doOnNext(cg -> {
                                                // 채팅방에 메시지 전송
                                                log.info("SEND MESSAGE TO ROOM");
                                                cidcRepo.getChannelIdChannelMap().get(c).flush();
                                                tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(b2.build());
                                            }));
                }).subscribe();

/*
        // filterMessage가 String을 반환하는 기존 상태
        // 위치 정보에서 비속어 필터링
        String place = messageService.filterMessage(seat.getContent());
        String time = LocalDateTime.now().toString();

        Transfer.Builder builder = Transfer.newBuilder();

        builder.setType("seat-win");
        builder.setContent(place);
        builder.setUserId(userId);
        builder.setSendAt(time);

        Transfer.Builder b2 = Transfer.newBuilder(builder.build())
                .setType("seat-end")
                .setContent("");

        redisTemplate.opsForHash().get(uPrefix + winnerId, "channel")
                .doOnNext(c -> {
                    // 당첨자 메시지 전송
                    log.info("SEND MESSAGE TO USER");
                    cidcRepo.getChannelIdChannelMap().get(c).writeAndFlush(builder.build());
                }).flatMap(c ->
                        redisTemplate.opsForHash().get(uPrefix + userId, "channelGroup")
                                .doOnNext(cg -> {
                                    // 채팅방에 메시지 전송
                                    log.info("SEND MESSAGE TO ROOM");
                                    cidcRepo.getChannelIdChannelMap().get(c).flush();
                                    tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(b2.build());
                                })
                )
                .subscribe();
*/

        // 이 형태는 2개 합친거 & 뒤에거 전송
//        // 당첨자 메시지 전송
//        messageService.sendMessageToOne(builder.build(), winnerId);
//
//        // 채팅방 전체 메시지 전송
//        messageService.sendMessageToRoom(b2.build(), "userId", userId);
    }

    public void villainOn(Transfer trans) {
        String userId = trans.getUserId();
        Mono.zip(redisTemplate.opsForHash().get(uPrefix + userId, "channelGroup"),
                        redisTemplate.opsForHash().get(uPrefix + userId, "channel"))
                .flatMap(tuple -> {
                            // 채널그룹 (열차번호)
                            String cg = (String) tuple.getT1();
                            // 채널 id
                            String c = (String) tuple.getT2();

                            // villain:열차번호를 찾음
                            return redisTemplate.opsForValue().get(vPrefix + cg)
                                    // 결과 데이터가 없다면 (빌런 추가가 가능한 시간)
                                    .switchIfEmpty(Mono.defer(() ->
                                            Mono.zip(redisTemplate.opsForValue().set(vPrefix + cg, "1"),
                                                            redisTemplate.expire(vPrefix + cg, Duration.ofMinutes(1)))
                                                    .flatMap(a -> redisTemplate.opsForHash().increment(tPrefix + cg, "villain", 1)
                                                            .flatMap(incre -> {
                                                                log.info("SEND VILLAIN ON");
                                                                Transfer.Builder send = Transfer.newBuilder(trans);
                                                                send.setContent(incre.toString());
                                                                send.build();
                                                                // 빌런 증가하면 증가한 숫자를 전송
                                                                tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(send);
                                                                // 종료
                                                                return Mono.empty();
                                                            }))
                                    ))
                                    // 결과 데이터가 있다면 (빌런 추가를 제한하는 시간)
                                    .flatMap(a -> {
                                        log.info("VILLAIN ADD PROHIBITED");
                                        Transfer.Builder send = Transfer.newBuilder(trans);
                                        send.setContent("-1");
                                        send.build();
                                        // 빌런 증가하면 증가한 숫자를 전송
                                        cidcRepo.getChannelIdChannelMap().get(c).writeAndFlush(send);

                                        return Mono.empty();
                                    });
                        }
                ).subscribe();
    }

    public void villainOff(Transfer trans) {
        redisTemplate.opsForHash().get(uPrefix + trans.getUserId(), "channelGroup")
                .flatMap(cg ->
                        redisTemplate.opsForHash().get(tPrefix + cg, "villain")
                                .flatMap(num -> {
                                    log.info("SEND VILLAIN OFF - num before : {}", num);

                                    int n = Integer.parseInt(String.valueOf(num));
                                    if (n == 0) {
                                        return Mono.empty();
                                    }

                                    Transfer send = Transfer.newBuilder(trans).setContent(String.valueOf(n - 1)).build();
                                    tcgRepo.getTrainChannelGroupMap().get(cg).writeAndFlush(send);

                                    return redisTemplate.opsForHash().put(tPrefix + cg, "villain", String.valueOf(n - 1));
                                })
                )
                .subscribe();
    }
}
