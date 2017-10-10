package com.tydic.mysql.async;

import com.tydic.mysql.AsyncListener;
import com.tydic.mysql.AsyncSocketChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by shihailong on 2017/9/21.
 */
public class UpdateCountListener extends AsyncListener<Integer> {
    public UpdateCountListener(AsyncSocketChannel asyncSocketChannel) {
        super(asyncSocketChannel);
    }

    public UpdateCountListener() {
        super();
    }

    @Override
    protected void channelReadOKPacket(ChannelHandlerContext ctx, ByteBuf ok) {
        int sw = ok.getByte(5) & 0xff;
        ok.skipBytes(5);
        switch (sw) {
            case 251:
                sw = -1;
                break;
            case 252:
                sw = ok.readShortLE();
                break;
            case 253:
                sw = ok.readMediumLE();
                break;
            case 254:
                sw = ok.readIntLE();
                break;
            default:
                break;
        }
        promise.setSuccess(sw);
    }
}
