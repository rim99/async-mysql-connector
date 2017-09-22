package com.tydic.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Created by shihailong on 2017/9/21.
 */
public class AsyncChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private volatile ByteBuf byteBuf;
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof ByteBuf){
            if(byteBuf == null){
                byteBuf = (ByteBuf) msg;
            }
        }
    }
}
