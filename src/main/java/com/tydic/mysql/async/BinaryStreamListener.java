package com.tydic.mysql.async;

import com.tydic.mysql.AsyncListener;
import com.tydic.mysql.AsyncSocketChannel;

/**
 * Created by shihailong on 2017/9/22.
 */
public abstract class BinaryStreamListener extends AsyncListener<Void> {

    public BinaryStreamListener() {
        super();
    }
    public void init(AsyncSocketChannel asyncSocketChannel){
        super.init(asyncSocketChannel);
    }
}
