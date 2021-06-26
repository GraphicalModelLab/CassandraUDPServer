package org.niwaiot.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UdpDecoderHandler extends MessageToMessageDecoder<DatagramPacket>  {
    private static final Logger LOGGER = LoggerFactory.getLogger(UdpDecoderHandler.class);

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket, List<Object> out) throws Exception {
        ByteBuf byteBuf = datagramPacket.content();
        byte[] data = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(data);
        String msg = new String(data);
        LOGGER.info("{}Received the news{}:" + msg);
        out.add(msg); //Pass the data to the next handler
    }
}