/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.frontend;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.frontend.netty.ServerHandlerInitializer;
import org.apache.shardingsphere.proxy.frontend.protocol.FrontDatabaseProtocolTypeFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * ShardingSphere-Proxy.
 */
@Slf4j
public final class ShardingSphereProxy {
    
    private final EventLoopGroup bossGroup;
    
    private final EventLoopGroup workerGroup;
    
    public ShardingSphereProxy() {
        bossGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        workerGroup = getWorkerGroup();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
    
    private EventLoopGroup getWorkerGroup() {
        int workerThreads = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.PROXY_FRONTEND_EXECUTOR_SIZE);
        return Epoll.isAvailable() ? new EpollEventLoopGroup(workerThreads) : new NioEventLoopGroup(workerThreads);
    }
    
    /**
     * Start ShardingSphere-Proxy.
     *
     * @param port port
     * @param addresses addresses
     */
    @SneakyThrows(InterruptedException.class)
    public void start(final int port, final List<String> addresses) {
        try {
            List<ChannelFuture> futures = startInternal(port, addresses);
            accept(futures);
        } finally {
            close();
        }
    }
    
    /**
     * Start ShardingSphere-Proxy with DomainSocket.
     *
     * @param socketPath socket path
     */
    public void start(final String socketPath) {
        if (!Epoll.isAvailable()) {
            log.error("Epoll is unavailable, DomainSocket can't start.");
            return;
        }
        ChannelFuture future = startDomainSocket(socketPath);
        future.addListener((ChannelFutureListener) futureParams -> {
            if (futureParams.isSuccess()) {
                log.info("The listening address for DomainSocket is {}", socketPath);
            } else {
                log.error("DomainSocket failed to start:{}", futureParams.cause().getMessage());
            }
        });
    }

    //启动netty
    private List<ChannelFuture> startInternal(final int port, final List<String> addresses) throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        initServerBootstrap(bootstrap);
        List<ChannelFuture> result = new ArrayList<>(addresses.size());
        for (String each : addresses) {
            // 将当前配置的Bootstrap实例绑定到指定的端口上，并同步等待操作完成
            // 参数 each 代表当前处理的Bootstrap实例
            // 参数 port 代表要绑定的端口号
            // 返回值为ChannelFuture实例，表示异步操作的结果
            result.add(bootstrap.bind(each, port).sync());
        }
        return result;
    }
    
    private ChannelFuture startDomainSocket(final String socketPath) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        initServerBootstrap(bootstrap, new DomainSocketAddress(socketPath));
        return bootstrap.bind();
    }
    
    private void accept(final List<ChannelFuture> futures) throws InterruptedException {
        log.info("ShardingSphere-Proxy {} mode started successfully", ProxyContext.getInstance().getContextManager().getComputeNodeInstanceContext().getModeConfiguration().getType());
      //启动成功后关闭通道
        for (ChannelFuture each : futures) {
            each.channel().closeFuture().sync();
        }
    }
    
    /**
     * 初始化Netty服务器配置
     * 此方法配置Netty服务器的启动参数，包括线程组、通道、选项和处理器
     *
     * @param bootstrap ServerBootstrap实例，用于配置服务器
     */
    private void initServerBootstrap(final ServerBootstrap bootstrap) {
        // 获取服务器backlog配置
        Integer backLog = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.PROXY_NETTY_BACKLOG);

        // 配置服务器的线程组和通道
        bootstrap.group(bossGroup, workerGroup)
                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                // 配置写缓冲区水位线，用于控制内存使用
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024 * 1024, 16 * 1024 * 1024))
                // 使用池化字节缓冲区分配器，提高内存使用效率
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                // 允许地址重用，方便在服务器重启时快速绑定端口
                .option(ChannelOption.SO_REUSEADDR, true)
                // 配置backlog参数，控制连接队列长度
                .option(ChannelOption.SO_BACKLOG, backLog)
                // 为子通道配置池化字节缓冲区分配器
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                // 开启TCP_NODELAY选项，减少网络延迟
                .childOption(ChannelOption.TCP_NODELAY, true)
                // 添加日志处理器，记录服务器启动和运行信息
                .handler(new LoggingHandler(LogLevel.INFO))
                // 添加服务器处理器初始化器，用于初始化处理器管道
                .childHandler(new ServerHandlerInitializer(FrontDatabaseProtocolTypeFactory.getDatabaseType()));
    }
    
    private void initServerBootstrap(final ServerBootstrap bootstrap, final DomainSocketAddress localDomainSocketAddress) {
        bootstrap.group(bossGroup, workerGroup)
                .channel(EpollServerDomainSocketChannel.class)
                .localAddress(localDomainSocketAddress)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ServerHandlerInitializer(FrontDatabaseProtocolTypeFactory.getDatabaseType()));
    }
    
    private void close() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        BackendExecutorContext.getInstance().getExecutorEngine().close();
        ProxyContext.getInstance().getContextManager().close();
    }
}
