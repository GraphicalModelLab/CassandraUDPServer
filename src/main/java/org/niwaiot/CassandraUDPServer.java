package org.niwaiot;

import org.niwaiot.handler.UdpDecoderHandler;
import org.niwaiot.handler.UdpEncoderHandler;
import org.niwaiot.handler.UdpHandler;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.netty.udp.UdpServer;

import java.time.Duration;

@SpringBootApplication
public class CassandraUDPServer {
    public static void main(String[] args) {
        SpringApplication.run(CassandraUDPServer.class, args);
    }

    @Bean
    CommandLineRunner serverRunner(UdpDecoderHandler udpDecoderHanlder, UdpEncoderHandler udpEncoderHandler, UdpHandler udpHandler) {
        return strings -> {
            createUdpServer(udpDecoderHanlder, udpEncoderHandler, udpHandler);
        };
    }

    /**
     * Create UDP Server
     *
     * @param udpDecoderHandler: a handler for parsing data reported by UDP Client
     * @param udpEncoderHandler: a handler used to send data to UDP Client for encoding
     * @param udpHandler:        The handler for users to maintain UDP links
     */
    private void createUdpServer(UdpDecoderHandler udpDecoderHandler, UdpEncoderHandler udpEncoderHandler, UdpHandler udpHandler) {
        UdpServer.create()
                .handle((in, out) -> {
                    in.receive()
                            .asByteArray()
                            .subscribe();
                    return Flux.never();
                })
                .port(8888) //UDP Server port
                .doOnBound(conn -> conn
                        .addHandler("decoder", udpDecoderHandler)
                        .addHandler("encoder", udpEncoderHandler)
                        .addHandler("handler", udpHandler)
                ) //You can add multiple handlers
                .bindNow(Duration.ofSeconds(30));
    }
}