//package com.o2a4.apigateway.filter;
//
//import org.springframework.cloud.gateway.filter.GatewayFilter;
//import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
//import org.springframework.stereotype.Component;
//
//@Component
//public class AuthorizationHeaderFilter extends AbstractGatewayFilterFactory<AuthorizationHeaderFilter.Config> {
//    @Autowired
//    private JwtUtil jwtUtil;
//
//    public AuthorizationHeaderFilter() {
//        super(Config.class);
//    }
//
//    public static class Config {
//        // application.yml 파일에서 지정한 filer의 Argument값을 받는 부분
//    }
//
//    @Override
//    public GatewayFilter apply(Config config) {
//        return (exchange, chain) -> {
//            String token = exchange.getRequest().getHeaders().get("Authorization").get(0).substring(7);   // 헤더의 토큰 파싱 (Bearer 제거)
//            Map<String, Object> userInfo = jwtUtil.getUserParseInfo(token);   // 파싱된 토큰의 claim을 추출해 아이디 값을 가져온다.
//
//            addAuthorizationHeaders(exchange.getRequest(), userInfo);
//
//            return chain.filter(exchange);
//        };
//    }
//
//    // 성공적으로 검증이 되었기 때문에 인증된 헤더로 요청을 변경해준다. 서비스는 해당 헤더에서 아이디를 가져와 사용한다.
//    private void addAuthorizationHeaders(ServerHttpRequest request, Map<String, Object> userInfo) {
//        request.mutate()
//                .header("X-Authorization-Id", userInfo.get("memberId").toString())
//                .build();
//    }
//
//    // 토큰 검증 요청을 실행하는 도중 예외가 발생했을 때 예외처리하는 핸들러
//    @Bean
//    public ErrorWebExceptionHandler tokenValidation() {
//        return new JwtTokenExceptionHandler();
//    }
//
//    // 실제 토큰이 null, 만료 등 예외 상황에 따른 예외처리
//    public class JwtTokenExceptionHandler implements ErrorWebExceptionHandler {
//        private String getErrorCode(int errorCode) {
//            return "{\\"errorCode\\":" + errorCode + "}";
//        }
//
//        @Override
//        public Mono<Void> handle(
//                ServerWebExchange exchange, Throwable ex) {
//            int errorCode = 500;
//            if (ex.getClass() == NullPointerException.class) {
//                errorCode = 100;
//            } else if (ex.getClass() == ExpiredJwtException.class) {
//                errorCode = 200;
//            }
//
//            byte[] bytes = getErrorCode(errorCode).getBytes(StandardCharsets.UTF_8);
//            DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
//            return exchange.getResponse().writeWith(Flux.just(buffer));
//        }
//    }
//}