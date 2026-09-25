"""client.py of an iClick Gateway."""
import asyncio
import logging
import random

from .const import HEARTBEAT_INTERVAL, MAX_RECONNECT_RETRIES, BASE_RECONNECT_DELAY, MAX_RECONNECT_DELAY

_LOGGER = logging.getLogger(__name__)

class BestjoyClient:
    def __init__(self, host: str, port: int, hub_id: str):
        self.host = host
        self.port = port
        self.hub_id = hub_id
        # 连接相关资源
        self._reader = None
        self._writer = None
        self._transport = None
        self._connection_ready = False  # 新增连接状态标志
        # 异步控制
        self._lock = asyncio.Lock()
        self._heartbeat_task = None
        self._reconnect_task = None
        # 重连策略
        self._reconnect_attempts = 0
        self._max_retries = MAX_RECONNECT_RETRIES          # 最大自动重试次数
        self._base_reconnect_delay = BASE_RECONNECT_DELAY
        self._max_reconnect_delay = MAX_RECONNECT_DELAY

    async def async_test_connection(self) -> bool:
        """测试连接（深度重置版）"""
        await self._hard_reset()  # 先执行硬重置
        try:
            async with self._lock:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=5
                )
                return True
        except Exception as e:
            _LOGGER.error(f"Connection test failed: {str(e)}")
            return False
        finally:
            await self._async_close()

    async def async_connect(self) -> bool:
        """建立连接。失败时只返回 False，由重连循环决定是否再试。"""
        async with self._lock:
            if self._connection_ready:
                return True

            try:
                # 等待网关初始化完成
                await asyncio.sleep(5)
                # 建立新连接
                _LOGGER.info(f"API try open_connection  {self.host}:{self.port}")
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=10
                )
                _LOGGER.info(f"API try open_connection success")
                # 启动心跳任务
                self._heartbeat_task = asyncio.create_task(
                    self._start_heartbeat(),
                    name=f"iclick_heartbeat_{self.host}"  # 增加主机标识
                )
                self._connection_ready = True
                self._reconnect_attempts = 0
                _LOGGER.info("Connection established")
                return True
            except Exception as e:
                _LOGGER.error(f"Connection failed: {str(e)}")
                self._connection_ready = False
                return False

    async def _start_heartbeat(self):
        """智能心跳任务"""
        try:
            while self._connection_ready:
                try:
                    self._writer.write(b'\x00')
                    await self._writer.drain()
                    await asyncio.sleep(60)
                except Exception as e:
                    _LOGGER.error(f"Heartbeat error: {str(e)}")
                    self._connection_ready = False
                    # 不要在心跳任务里 await 重连：重连会关闭并等待本任务。
                    self._schedule_reconnect()
                    break
        except asyncio.CancelledError:
            _LOGGER.debug("Heartbeat cancelled")

    async def async_send_command(self, data: str):  # 改为直接接收data字符串
        """直接发送原始指令（不进行协议封装）"""
        for attempt in range(3):  # <-- 错误发生位置
            if not self._connection_ready:
                _LOGGER.warning(f"Hub {self.hub_id} 连接未就绪（尝试 {attempt+1}/3）")
                await self.async_reconnect()
                if not self._connection_ready:
                    break
                continue
            try:
                # 直接发送原始数据（不进行协议封装）
                bytes_data = bytes.fromhex(data)
                async with self._lock:
                    self._writer.write(bytes_data)
                    await asyncio.wait_for(self._writer.drain(), timeout=5)
                    return
            except Exception as e:
                _LOGGER.error(f"Hub {self.hub_id} 发送失败：{str(e)}")
                self._connection_ready = False
                await self.async_reconnect()
        _LOGGER.error(f"Hub {self.hub_id} 所有发送尝试均失败")
        await self._hard_reset()

    def _schedule_reconnect(self) -> None:
        """保证同一网关只有一个重连任务。"""
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(
            self._reconnect_loop(),
            name=f"iclick_reconnect_{self.hub_id}",
        )

    async def async_reconnect(self) -> None:
        """等待当前重连结束。心跳任务只调度，不在这里等待。"""
        current = asyncio.current_task()
        if self._reconnect_task and not self._reconnect_task.done():
            if self._reconnect_task is current:
                return
            await self._reconnect_task
            return
        self._schedule_reconnect()
        if self._reconnect_task is not current:
            await self._reconnect_task

    async def _reconnect_loop(self) -> None:
        """按次数退避重连。连接失败会计次，而不是每次都从 1/5 重新开始。"""
        self._connection_ready = False
        await self._async_close()

        while self._reconnect_attempts < self._max_retries:
            delay = self._calc_retry_delay()
            attempt = self._reconnect_attempts + 1
            _LOGGER.warning(
                f"Hub {self.hub_id} reconnect attempt {attempt}/{self._max_retries}, delay={delay:.1f}"
            )
            try:
                await asyncio.sleep(delay)
                if await self.async_connect():
                    return
            except Exception as e:
                _LOGGER.error(f"Reconnect error: {str(e)}")
            self._reconnect_attempts = attempt

        _LOGGER.error(
            f"Hub {self.hub_id} reconnect failed after {self._max_retries} attempts"
        )
        await self._hard_reset()

    def _calc_retry_delay(self) -> float:
        """计算退避时间（含随机抖动）"""
        base_delay = min(
            self._base_reconnect_delay * (2 ** self._reconnect_attempts),
            self._max_reconnect_delay
        )
        return base_delay + random.uniform(0, 2)

    async def _hard_reset(self):
        """只重置当前网关，再尝试连接一次。"""
        _LOGGER.warning(f"Hub {self.hub_id} performing hard reset")
        await self._async_close()
        self._reconnect_attempts = 0
        self._connection_ready = False
        await asyncio.sleep(1)  # 等待资源释放
        await self.async_connect()

    async def _async_close(self):
        """原子化关闭操作"""
        # 关闭心跳任务。重连若由心跳触发，不能等待当前任务自己结束。
        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat and not heartbeat.done() and heartbeat is not asyncio.current_task():
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
                
        # 关闭网络连接
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception as e:
                _LOGGER.debug(f"Close error: {str(e)}")
            finally:
                self._writer = None
                self._reader = None
                
        if self._transport:
            self._transport.close()
            try:
                await self._transport.wait_closed()
            except Exception:
                pass
            finally:
                self._transport = None